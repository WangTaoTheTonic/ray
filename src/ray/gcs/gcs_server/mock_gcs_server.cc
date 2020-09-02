// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/gcs/gcs_server/mock_gcs_server.h"

#include "ray/common/network_util.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/error_info_handler_impl.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_job_manager.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_object_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_worker_manager.h"
#include "ray/gcs/gcs_server/stats_handler_impl.h"
#include "ray/gcs/gcs_server/task_info_handler_impl.h"

#include "ray/common/test_util.h"

namespace ray {
namespace gcs {

MockGcsServer::MockGcsServer(const ray::gcs::GcsServerConfig &config,
                             boost::asio::io_service &main_service)
    : config_(config),
      main_service_(main_service),
      rpc_server_(config.grpc_server_name, config.grpc_server_port,
                  config.grpc_server_thread_num),
      client_call_manager_(main_service) {}

MockGcsServer::~MockGcsServer() { Stop(); }

void MockGcsServer::Start() {
  // Init backend client.
  InitBackendClient();

  // Init gcs pub sub instance.
  gcs_pub_sub_ = std::make_shared<gcs::GcsPubSub>(redis_gcs_client_->GetRedisClient());

  // Init gcs table storage.
  gcs_table_storage_ =
      std::make_shared<gcs::RedisGcsTableStorage>(redis_gcs_client_->GetRedisClient());

  // Init gcs node_manager.
  InitGcsNodeManager();

  // Init gcs detector.
  gcs_redis_failure_detector_ = std::make_shared<GcsRedisFailureDetector>(
      main_service_, redis_gcs_client_->primary_context(), [this]() { Stop(); });
  gcs_redis_failure_detector_->Start();

  // Init gcs actor manager.
  InitGcsActorManager();

  // Init gcs placement group manager.
  InitGcsPlacementGroupManager();

  // Register rpc service.
  gcs_object_manager_ = InitObjectManager();
  object_info_service_.reset(
      new rpc::ObjectInfoGrpcService(main_service_, *gcs_object_manager_));
  rpc_server_.RegisterService(*object_info_service_);

  task_info_handler_ = InitTaskInfoHandler();
  task_info_service_.reset(
      new rpc::TaskInfoGrpcService(main_service_, *task_info_handler_));
  rpc_server_.RegisterService(*task_info_service_);

  InitGcsJobManager();
  job_info_service_.reset(new rpc::JobInfoGrpcService(main_service_, *gcs_job_manager_));
  rpc_server_.RegisterService(*job_info_service_);

  actor_info_service_.reset(
      new rpc::ActorInfoGrpcService(main_service_, *gcs_actor_manager_));
  rpc_server_.RegisterService(*actor_info_service_);

  placement_group_info_service_.reset(new rpc::PlacementGroupInfoGrpcService(
      main_service_, *gcs_placement_group_manager_));
  rpc_server_.RegisterService(*placement_group_info_service_);

  node_info_service_.reset(
      new rpc::NodeInfoGrpcService(main_service_, *gcs_node_manager_));
  rpc_server_.RegisterService(*node_info_service_);

  stats_handler_ = InitStatsHandler();
  stats_service_.reset(new rpc::StatsGrpcService(main_service_, *stats_handler_));
  rpc_server_.RegisterService(*stats_service_);

  error_info_handler_ = InitErrorInfoHandler();
  error_info_service_.reset(
      new rpc::ErrorInfoGrpcService(main_service_, *error_info_handler_));
  rpc_server_.RegisterService(*error_info_service_);

  gcs_worker_manager_ = InitGcsWorkerManager();
  worker_info_service_.reset(
      new rpc::WorkerInfoGrpcService(main_service_, *gcs_worker_manager_));
  rpc_server_.RegisterService(*worker_info_service_);

  auto load_completed_count = std::make_shared<int>(0);
  int load_count = 2;
  auto on_done = [this, load_count, load_completed_count]() {
    ++(*load_completed_count);

    // We will reschedule the unfinished actors, so we have to load the actor data at the
    // end to make sure the other table data is loaded.
    if (*load_completed_count == load_count) {
      auto actor_manager_load_initial_data_callback = [this]() {
        // Start RPC server when all tables have finished loading initial data.
        rpc_server_.Run();

        // Store gcs rpc server address in redis.
        StoreGcsServerAddressInRedis();

        // Only after the rpc_server_ is running can the node failure detector be run.
        // Otherwise the node failure detector will mistake some living nodes as dead
        // as the timer inside node failure detector is already run.
        gcs_node_manager_->StartNodeFailureDetector();
        is_started_ = true;
      };
      gcs_actor_manager_->LoadInitialData(actor_manager_load_initial_data_callback);
    }
  };
  gcs_object_manager_->LoadInitialData(on_done);
  gcs_node_manager_->LoadInitialData(on_done);
}

void MockGcsServer::Stop() {
  if (!is_stopped_) {
    RAY_LOG(INFO) << "Stopping GCS server.";
    // Shutdown the rpc server
    rpc_server_.Shutdown();

    is_stopped_ = true;
    RAY_LOG(INFO) << "GCS server stopped.";
  }
}

void MockGcsServer::InitBackendClient() {
  GcsClientOptions options(config_.redis_address, config_.redis_port,
                           config_.redis_password, config_.is_test);
  redis_gcs_client_ = std::make_shared<RedisGcsClient>(options);
  auto status = redis_gcs_client_->Connect(main_service_);
  RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;
}

void MockGcsServer::InitGcsNodeManager() {
  RAY_CHECK(redis_gcs_client_ != nullptr);
  gcs_node_manager_ = std::make_shared<GcsNodeManager>(
      main_service_, redis_gcs_client_->Errors(), gcs_pub_sub_, gcs_table_storage_);
}

class MockRayletClient : public WorkerLeaseInterface {
 public:
  ray::Status RequestWorkerLease(
      const ray::TaskSpecification &resource_spec,
      const rpc::ClientCallback<rpc::RequestWorkerLeaseReply> &callback) override {
    rpc::RequestWorkerLeaseReply reply;
    auto worker_id = workers_.at(index_);
    index_++;
    reply.mutable_retry_at_raylet_address()->set_ip_address("address");
    reply.mutable_retry_at_raylet_address()->set_port(0);
    reply.mutable_retry_at_raylet_address()->set_raylet_id(node_id_);
    reply.mutable_worker_address()->set_ip_address("address");
    reply.mutable_worker_address()->set_port(0);
    reply.mutable_worker_address()->set_raylet_id(node_id_);
    reply.mutable_worker_address()->set_worker_id(worker_id);
    callback(Status::OK(), reply);

    return Status::OK();
  }

  ray::Status ReturnWorker(int worker_port, const WorkerID &worker_id,
                           bool disconnect_worker) override {
    return Status::OK();
  }

  ray::Status ReleaseUnusedWorkers(
      const std::vector<WorkerID> &workers_in_use,
      const rpc::ClientCallback<rpc::ReleaseUnusedWorkersReply> &callback) override {
    return Status::NotImplemented("ReleaseUnusedWorkers is not supported.");
  }

  ray::Status CancelWorkerLease(
      const TaskID &task_id,
      const rpc::ClientCallback<rpc::CancelWorkerLeaseReply> &callback) override {
    return Status::OK();
  }

  MockRayletClient(std::string node_id, std::vector<std::string> workers)
      : node_id_(node_id), workers_(workers) {
    index_ = 0;
  }
  ~MockRayletClient() {}

  std::string node_id_;
  std::vector<std::string> workers_;
  uint32_t index_;
};

class MockWorkerClient : public rpc::CoreWorkerClientInterface {
 public:
  ray::Status PushNormalTask(
      std::unique_ptr<rpc::PushTaskRequest> request,
      const rpc::ClientCallback<rpc::PushTaskReply> &callback) override {
    auto reply = rpc::PushTaskReply();
    callback(Status::OK(), reply);
    return Status::OK();
  }

  ray::Status WaitForActorOutOfScope(
      const rpc::WaitForActorOutOfScopeRequest &request,
      const rpc::ClientCallback<rpc::WaitForActorOutOfScopeReply> &callback) override {
    return Status::OK();
  }
};

class MockActorScheduler : public gcs::GcsActorScheduler {
 public:
  using gcs::GcsActorScheduler::GcsActorScheduler;

  /// Get an existing lease client or connect a new one.
  std::shared_ptr<WorkerLeaseInterface> GetOrConnectLeaseClient(
      const rpc::Address &raylet_address) override {
    auto node_id = ClientID::FromBinary(raylet_address.raylet_id());
    auto iter = remote_lease_clients_.find(node_id);
    if (iter == remote_lease_clients_.end()) {
      auto lease_client = std::make_shared<MockRayletClient>(
          node_id.Binary(), TestSetupUtil::GetWorkers(node_id.Binary()));
      iter = remote_lease_clients_.emplace(node_id, std::move(lease_client)).first;
    }
    return iter->second;
  }

  /// Get or create CoreWorkerClient to communicate with the remote leased worker.
  std::shared_ptr<rpc::CoreWorkerClientInterface> GetOrConnectCoreWorkerClient(
      const rpc::Address &worker_address) override {
    auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
    auto iter = core_worker_clients_.find(worker_id);
    if (iter == core_worker_clients_.end()) {
      iter = core_worker_clients_.emplace(worker_id, std::make_shared<MockWorkerClient>())
                 .first;
    }
    return iter->second;
  }
};

void MockGcsServer::InitGcsActorManager() {
  RAY_CHECK(gcs_table_storage_ != nullptr && gcs_node_manager_ != nullptr);
  auto scheduler = std::make_shared<MockActorScheduler>(
      main_service_, gcs_table_storage_->ActorTable(), *gcs_node_manager_, gcs_pub_sub_,
      /*schedule_failure_handler=*/
      [this](std::shared_ptr<GcsActor> actor) {
        // When there are no available nodes to schedule the actor the
        // gcs_actor_scheduler will treat it as failed and invoke this handler. In
        // this case, the actor manager should schedule the actor once an
        // eligible node is registered.
        gcs_actor_manager_->OnActorCreationFailed(std::move(actor));
      },
      /*schedule_success_handler=*/
      [this](std::shared_ptr<GcsActor> actor) {
        gcs_actor_manager_->OnActorCreationSuccess(std::move(actor));
      },
      /*lease_client_factory=*/
      [this](const rpc::Address &address) {
        auto node_manager_worker_client = rpc::NodeManagerWorkerClient::make(
            address.ip_address(), address.port(), client_call_manager_);
        return std::make_shared<ray::raylet::RayletClient>(
            std::move(node_manager_worker_client));
      },
      /*client_factory=*/
      [this](const rpc::Address &address) {
        return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
      });
  gcs_actor_manager_ = std::make_shared<GcsActorManager>(
      scheduler, gcs_table_storage_, gcs_pub_sub_, [this](const rpc::Address &address) {
        return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
      });
  gcs_node_manager_->AddNodeAddedListener(
      [this](const std::shared_ptr<rpc::GcsNodeInfo> &) {
        // Because a new node has been added, we need to try to schedule the pending
        // actors.
        gcs_actor_manager_->SchedulePendingActors();
      });

  gcs_node_manager_->AddNodeRemovedListener(
      [this](std::shared_ptr<rpc::GcsNodeInfo> node) {
        // All of the related actors should be reconstructed when a node is removed from
        // the GCS.
        gcs_actor_manager_->OnNodeDead(ClientID::FromBinary(node->node_id()));
      });

  auto on_subscribe = [this](const std::string &id, const std::string &data) {
    rpc::WorkerTableData worker_failure_data;
    worker_failure_data.ParseFromString(data);
    auto &worker_address = worker_failure_data.worker_address();
    WorkerID worker_id = WorkerID::FromBinary(id);
    ClientID node_id = ClientID::FromBinary(worker_address.raylet_id());
    gcs_actor_manager_->OnWorkerDead(node_id, worker_id,
                                     worker_failure_data.intentional_disconnect());
  };
  RAY_CHECK_OK(gcs_pub_sub_->SubscribeAll(WORKER_CHANNEL, on_subscribe, nullptr));
}

void MockGcsServer::InitGcsJobManager() {
  gcs_job_manager_ =
      std::unique_ptr<GcsJobManager>(new GcsJobManager(gcs_table_storage_, gcs_pub_sub_));
  gcs_job_manager_->AddJobFinishedListener([this](std::shared_ptr<JobID> job_id) {
    gcs_actor_manager_->OnJobFinished(*job_id);
  });
}

void MockGcsServer::InitGcsPlacementGroupManager() {
  RAY_CHECK(gcs_table_storage_ != nullptr && gcs_node_manager_ != nullptr);
  auto scheduler = std::make_shared<GcsPlacementGroupScheduler>(
      main_service_, gcs_table_storage_, *gcs_node_manager_,
      /*lease_client_factory=*/
      [this](const rpc::Address &address) {
        auto node_manager_worker_client = rpc::NodeManagerWorkerClient::make(
            address.ip_address(), address.port(), client_call_manager_);
        return std::make_shared<ray::raylet::RayletClient>(
            std::move(node_manager_worker_client));
      });

  gcs_placement_group_manager_ = std::make_shared<GcsPlacementGroupManager>(
      main_service_, scheduler, gcs_table_storage_);
}

std::unique_ptr<GcsObjectManager> MockGcsServer::InitObjectManager() {
  return std::unique_ptr<GcsObjectManager>(
      new GcsObjectManager(gcs_table_storage_, gcs_pub_sub_, *gcs_node_manager_));
}

void MockGcsServer::StoreGcsServerAddressInRedis() {
  std::string address =
      GetValidLocalIp(
          GetPort(),
          RayConfig::instance().internal_gcs_service_connect_wait_milliseconds()) +
      ":" + std::to_string(GetPort());
  RAY_LOG(INFO) << "Gcs server address = " << address;

  RAY_CHECK_OK(redis_gcs_client_->primary_context()->RunArgvAsync(
      {"SET", "GcsServerAddress", address}));
  RAY_LOG(INFO) << "Finished setting gcs server address: " << address;
}

std::unique_ptr<rpc::TaskInfoHandler> MockGcsServer::InitTaskInfoHandler() {
  return std::unique_ptr<rpc::DefaultTaskInfoHandler>(
      new rpc::DefaultTaskInfoHandler(gcs_table_storage_, gcs_pub_sub_));
}

std::unique_ptr<rpc::StatsHandler> MockGcsServer::InitStatsHandler() {
  return std::unique_ptr<rpc::DefaultStatsHandler>(
      new rpc::DefaultStatsHandler(gcs_table_storage_));
}

std::unique_ptr<rpc::ErrorInfoHandler> MockGcsServer::InitErrorInfoHandler() {
  return std::unique_ptr<rpc::DefaultErrorInfoHandler>(
      new rpc::DefaultErrorInfoHandler(*redis_gcs_client_));
}

std::unique_ptr<GcsWorkerManager> MockGcsServer::InitGcsWorkerManager() {
  return std::unique_ptr<GcsWorkerManager>(
      new GcsWorkerManager(gcs_table_storage_, gcs_pub_sub_));
}

}  // namespace gcs
}  // namespace ray
