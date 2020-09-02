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

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/mock_gcs_server.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"

namespace ray {

class GcsServerStressTest : public ::testing::Test {
 public:
  GcsServerStressTest() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  virtual ~GcsServerStressTest() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    gcs::GcsServerConfig config;
    config.grpc_server_port = 0;
    config.grpc_server_name = "MockedGcsServer";
    config.grpc_server_thread_num = 1;
    config.redis_address = "127.0.0.1";
    config.is_test = true;
    config.redis_port = TEST_REDIS_SERVER_PORTS.front();
    gcs_server_.reset(new gcs::MockGcsServer(config, io_service_));
    gcs_server_->Start();

    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    // Wait until server starts listening.
    while (gcs_server_->GetPort() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  void TearDown() override {
    gcs_server_->Stop();
    io_service_.stop();
    gcs_server_.reset();
    thread_io_service_->join();
  }

  bool AddJob(const rpc::AddJobRequest &request) {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::promise<bool> promise;
    client_->AddJob(request,
                    [&promise](const Status &status, const rpc::AddJobReply &reply) {
                      RAY_CHECK_OK(status);
                      promise.set_value(true);
                    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool MarkJobFinished(const rpc::MarkJobFinishedRequest &request) {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::promise<bool> promise;
    client_->MarkJobFinished(request, [&promise](const Status &status,
                                                 const rpc::MarkJobFinishedReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterActor(std::shared_ptr<rpc::GcsRpcClient> client_,
                     const rpc::RegisterActorRequest &request) {
    std::promise<bool> promise;
    client_->RegisterActor(
        request, [&promise](const Status &status, const rpc::RegisterActorReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterActorInfo(std::shared_ptr<rpc::GcsRpcClient> client_,
                         const rpc::RegisterActorInfoRequest &request) {
    std::promise<bool> promise;
    client_->RegisterActorInfo(
        request,
        [&promise](const Status &status, const rpc::RegisterActorInfoReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool CreateActor(std::shared_ptr<rpc::GcsRpcClient> client_,
                   const rpc::CreateActorRequest &request) {
    std::promise<bool> promise;
    client_->CreateActor(
        request, [&promise](const Status &status, const rpc::CreateActorReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool RegisterNode(const rpc::RegisterNodeRequest &request) {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::promise<bool> promise;
    client_->RegisterNode(
        request, [&promise](const Status &status, const rpc::RegisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UnregisterNode(const rpc::UnregisterNodeRequest &request) {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::promise<bool> promise;
    client_->UnregisterNode(
        request, [&promise](const Status &status, const rpc::UnregisterNodeReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  std::vector<rpc::GcsNodeInfo> GetAllNodeInfo() {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::vector<rpc::GcsNodeInfo> node_info_list;
    rpc::GetAllNodeInfoRequest request;
    std::promise<bool> promise;
    client_->GetAllNodeInfo(
        request, [&node_info_list, &promise](const Status &status,
                                             const rpc::GetAllNodeInfoReply &reply) {
          RAY_CHECK_OK(status);
          for (int index = 0; index < reply.node_info_list_size(); ++index) {
            node_info_list.push_back(reply.node_info_list(index));
          }
          promise.set_value(true);
        });
    EXPECT_TRUE(WaitReady(promise.get_future(), timeout_ms_));
    return node_info_list;
  }

  bool AddWorkerInfo(const rpc::AddWorkerInfoRequest &request) {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::promise<bool> promise;
    client_->AddWorkerInfo(
        request, [&promise](const Status &status, const rpc::AddWorkerInfoReply &reply) {
          RAY_CHECK_OK(status);
          promise.set_value(true);
        });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool ReportHeartbeat(const rpc::ReportHeartbeatRequest &request) {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::promise<bool> promise;
    client_->ReportHeartbeat(request, [&promise](const Status &status,
                                                 const rpc::ReportHeartbeatReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool UpdateResources(const rpc::UpdateResourcesRequest &request) {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::promise<bool> promise;
    client_->UpdateResources(request, [&promise](const Status &status,
                                                 const rpc::UpdateResourcesReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool DeleteResources(const rpc::DeleteResourcesRequest &request) {
    std::shared_ptr<rpc::GcsRpcClient> client_ = clients_.front();
    std::promise<bool> promise;
    client_->DeleteResources(request, [&promise](const Status &status,
                                                 const rpc::DeleteResourcesReply &reply) {
      RAY_CHECK_OK(status);
      promise.set_value(true);
    });
    return WaitReady(promise.get_future(), timeout_ms_);
  }

  bool WaitReady(const std::future<bool> &future, uint64_t timeout_ms) {
    auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
    return status == std::future_status::ready;
  }

  void InitClients(uint32_t clients_number) {
    for (int i = 0; i < clients_number; i++) {
      // Create gcs rpc clients
      std::shared_ptr<rpc::ClientCallManager> client_call_manager_ =
          std::make_shared<rpc::ClientCallManager>(io_service_);
      client_call_managers_.push_back(client_call_manager_);
      clients_.push_back(std::make_shared<rpc::GcsRpcClient>(
          "0.0.0.0", gcs_server_->GetPort(), *client_call_manager_));
    }
  }

  void RegisterNodes(uint32_t nodes_number) {
    for (int i = 0; i < nodes_number; i++) {
      // Create gcs node info
      auto gcs_node_info = Mocker::GenNodeInfo();

      // Register node info
      rpc::RegisterNodeRequest register_node_info_request;
      register_node_info_request.mutable_node_info()->CopyFrom(*gcs_node_info);
      ASSERT_TRUE(RegisterNode(register_node_info_request));

      // Update node resources
      rpc::UpdateResourcesRequest update_resources_request;
      update_resources_request.set_node_id(gcs_node_info->node_id());
      rpc::ResourceTableData resource_table_data;
      resource_table_data.set_resource_capacity(1000.0);
      std::string resource_name = "CPU";
      (*update_resources_request.mutable_resources())[resource_name] =
          resource_table_data;
      ASSERT_TRUE(UpdateResources(update_resources_request));
      node_ids.push_back(std::move(gcs_node_info->node_id()));
    }
    ASSERT_TRUE(node_ids.size() == nodes_number);
  }

  void AddWorkers(uint32_t workers_number_per_node) {
    for (std::vector<std::string>::iterator it = node_ids.begin(); it != node_ids.end();
         ++it) {
      TestSetupUtil::InitWorkers(*it, workers_number_per_node);
    }
    for (std::vector<std::string>::iterator it = node_ids.begin(); it != node_ids.end();
         ++it) {
      for (int i = 0; i < workers_number_per_node; i++) {
        // Add worker info
        auto worker_data = Mocker::GenWorkerTableData();
        auto worker_id = WorkerID::FromRandom().Binary();
        worker_data->mutable_worker_address()->set_worker_id(worker_id);
        worker_data->mutable_worker_address()->set_raylet_id(*it);
        rpc::AddWorkerInfoRequest add_worker_request;
        add_worker_request.mutable_worker_data()->CopyFrom(*worker_data);
        ASSERT_TRUE(AddWorkerInfo(add_worker_request));
        TestSetupUtil::AddWorker(*it, worker_id);
      }
    }
  }

  void RegisterActors(uint32_t actors_per_client) {
    for (auto it = node_ids.begin(); it != node_ids.end(); ++it) {
      auto worker_ids = TestSetupUtil::GetWorkers(*it);
      int number = 0;
      for (auto jt = worker_ids.begin(); jt != worker_ids.end(); ++jt, ++number) {
        if (number >= worker_ids.size() / 2) {
          break;
        }
        JobID job_id = JobID::FromInt(1);

        auto register_actor_request = Mocker::GenRegisterActorRequest(job_id, 0, true);
        register_actor_request.mutable_task_spec()
            ->mutable_caller_address()
            ->set_raylet_id(*it);
        register_actor_request.mutable_task_spec()
            ->mutable_caller_address()
            ->set_worker_id(*jt);

        register_actor_requests.push_back(register_actor_request);
      }
    }
    for (int i = 0; i < clients_.size(); i++) {
      register_threads_.push_back(std::thread(
          [this, i, actors_per_client]() { DoRegisterActors(i, actors_per_client); }));
    }
  }

  void DoRegisterActors(int index, int step) {
    auto client_ = clients_.at(index);
    for (int i = index * step; i < (index + 1) * step; i++) {
      RegisterActor(client_, register_actor_requests.at(i));
    }
  }

  void CreateActors() {
    uint32_t step = register_actor_requests.size() / clients_.size();
    for (int i = 0; i < clients_.size(); i++) {
      create_threads_.push_back(std::thread([this, i, step]() {
        DoCreateActors(clients_.at(i), i * step, (i + 1) * step);
      }));
    }
  }

  void DoCreateActors(std::shared_ptr<rpc::GcsRpcClient> client_, uint32_t start,
                      uint32_t end) {
    JobID job_id = JobID::FromInt(1);
    for (int i = start; i < end; i++) {
      auto it = register_actor_requests.at(i);

      // Create actor.
      auto create_actor_request = Mocker::GenCreateActorRequest(job_id);
      create_actor_request.mutable_task_spec()
          ->mutable_actor_creation_task_spec()
          ->set_actor_id(it.task_spec().actor_creation_task_spec().actor_id());
      create_actor_request.mutable_task_spec()->mutable_caller_address()->set_raylet_id(
          it.task_spec().caller_address().raylet_id());
      create_actor_request.mutable_task_spec()->mutable_caller_address()->set_worker_id(
          it.task_spec().caller_address().worker_id());

      CreateActor(client_, create_actor_request);
    }
  }

  void WaitRegisterDone() {
    for (auto iter = register_threads_.begin(); iter != register_threads_.end(); ++iter) {
      iter->join();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  void WaitCreateDone() {
    for (auto iter = create_threads_.begin(); iter != create_threads_.end(); ++iter) {
      iter->join();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

 protected:
  // Gcs server
  std::unique_ptr<gcs::MockGcsServer> gcs_server_;
  std::unique_ptr<std::thread> thread_io_service_;
  boost::asio::io_service io_service_;

  // Gcs client
  std::vector<std::shared_ptr<rpc::GcsRpcClient>> clients_;
  std::vector<std::shared_ptr<rpc::ClientCallManager>> client_call_managers_;

  // Timeout waiting for gcs server reply, default is 5s
  const uint64_t timeout_ms_ = 5000;

  std::vector<std::string> node_ids;
  std::vector<rpc::RegisterActorRequest> register_actor_requests;
  std::vector<std::thread> register_threads_;
  std::vector<std::thread> create_threads_;
};

TEST_F(GcsServerStressTest, TestActorInfo) {
  uint32_t actor_number = 5000;
  uint32_t client_number = 20;
  uint32_t node_number = 10;

  uint32_t worker_number_per_node = actor_number / node_number * 2;
  uint32_t actor_requests_per_client = actor_number / client_number;

  /// Assume we have m nodes and n actors in total, then #workers per node = n / m * 2.

  /// - Init `client_number` gcs clients.
  /// - Register `node_number` nodes with first gcs client.
  /// - Add `worker_number_per_node` for each node with first client.
  /// - Register `worker_number_per_node / 2` actor for each node. Each client send
  /// `node_number * worker_number_per_node / 2 / client_number` request.
  /// - Create `worker_number_per_node / 2` actor for each node. Each client send
  /// `node_number * worker_number_per_node / 2 / client_number` request.
  InitClients(client_number);
  RegisterNodes(node_number);
  AddWorkers(worker_number_per_node);
  RegisterActors(actor_requests_per_client);
  WaitRegisterDone();
  CreateActors();
  WaitCreateDone();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::TEST_REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
