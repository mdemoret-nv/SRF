/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable @typescript-eslint/require-await */
/* eslint-disable @typescript-eslint/no-non-null-asserted-optional-chain */
/* eslint-disable @typescript-eslint/no-non-null-assertion */

import { MrcTestClient } from "@mrc/client/client";
import { ConnectionManager } from "@mrc/client/connection_manager";
import { PipelineManager } from "@mrc/client/pipeline_manager";
import { WorkersManager } from "@mrc/client/workers_manager";
import { IPipelineConfiguration } from "@mrc/common/entities";
import { hashName16 } from "@mrc/common/utils";
import { PingRequest } from "@mrc/proto/mrc/protos/architect";
import {
   ManifoldOptions_Policy,
   PipelineInstance,
   ResourceActualStatus,
   ResourceRequestedStatus,
} from "@mrc/proto/mrc/protos/architect_state";
import { connectionsSelectAll, connectionsSelectById } from "@mrc/server/store/slices/connectionsSlice";
import { pipelineDefinitionsSelectById } from "@mrc/server/store/slices/pipelineDefinitionsSlice";
import { pipelineInstancesSelectById } from "@mrc/server/store/slices/pipelineInstancesSlice";
import { workersSelectById } from "@mrc/server/store/slices/workersSlice";

describe("Connection", () => {
   const client: MrcTestClient = new MrcTestClient();

   beforeEach(async () => {
      await client.initializeClient();
   });

   afterEach(async () => {
      await client.finalizeClient();
   });

   test("Is Ready", () => {
      expect(client.isChannelConnected()).toBeTruthy();
   });

   test("Ping", async () => {
      const req = PingRequest.create({
         tag: "1234",
      });

      const resp = await client.ping(req);

      expect(resp.tag).toBe(req.tag);
   });

   test("No Connections On Start", async () => {
      expect(connectionsSelectAll(client.getServerState())).toHaveLength(0);
   });

   test("No Connections After Disconnect", async () => {
      // Connect then disconnect
      const manager = ConnectionManager.create(client);
      await manager.register();
      await manager.unregister();

      // Should have 0 connections in the state
      expect(connectionsSelectAll(client.getServerState())).toHaveLength(0);
   });

   describe("With EventStream", () => {
      const connectionManager = ConnectionManager.create(client);

      beforeEach(async () => {
         await connectionManager.ensureResourcesCreated();
      });

      afterEach(async () => {
         await connectionManager.unregister();
      });

      test("Found Connection", async () => {
         // Verify the number of connections is 1
         const connection = connectionsSelectById(client.getServerState(), connectionManager.connectionId);

         expect(connection).toBeDefined();
         expect(connection?.id).toEqual(connectionManager.connectionId);
      });

      // test("Abort", async () => {
      //    expect(client.abortConnection()).rejects.toThrow("The operation has been aborted");
      // });
   });
});

describe("Worker", () => {
   const client: MrcTestClient = new MrcTestClient();
   const connectionManager = ConnectionManager.create(client);

   beforeEach(async () => {
      await connectionManager.ensureResourcesCreated();
   });

   afterEach(async () => {
      await connectionManager.unregister();
      await client.finalizeClient();
   });

   test("Add One", async () => {
      const manager = new WorkersManager(connectionManager, ["test data"]);

      await manager.register();

      expect(manager.executorId).toBe(connectionManager.connectionId);

      // Need to do deeper checking here
   });

   test("Activate", async () => {
      const manager = new WorkersManager(connectionManager, ["test data"]);

      await manager.createResources();

      // Check to make sure its activated
      let found_worker = workersSelectById(client.getServerState(), manager.workerIds[0]);

      expect(found_worker?.state.actualStatus).toBe(ResourceActualStatus.Actual_Created);

      // Now set it as running
      await manager.runResources();

      // Ensure its running
      found_worker = workersSelectById(client.getServerState(), manager.workerIds[0]);

      expect(found_worker?.state.actualStatus).toBe(ResourceActualStatus.Actual_Running);
   });
});

describe("Pipeline", () => {
   const client: MrcTestClient = new MrcTestClient();
   const workersManager: WorkersManager = WorkersManager.create(["test data", "test data 2"], client);

   beforeEach(async () => {
      // Ensure everything up to the workers is ready to go
      await workersManager.ensureResourcesCreated();
   });

   afterEach(async () => {
      await workersManager.unregister();
      await client.finalizeClient();
   });

   test("Request Assignment", async () => {
      const pipeline_config: IPipelineConfiguration = {
         segments: {
            my_seg1: {
               name: "my_seg",
               nameHash: hashName16("my_seg"),
               egressPorts: [],
               ingressPorts: [],
            },
            my_seg2: {
               name: "my_seg2",
               nameHash: hashName16("my_seg2"),
               egressPorts: [],
               ingressPorts: [],
            },
         },
         manifolds: {},
      };

      const manager = new PipelineManager(workersManager, pipeline_config);

      // Now request to run a pipeline
      await manager.register();

      // Check the pipeline definition
      const foundPipelineDefinition = pipelineDefinitionsSelectById(
         client.getServerState(),
         manager.pipelineDefinitionId
      );

      expect(foundPipelineDefinition?.id).toBe(manager.pipelineDefinitionId);

      // Check pipeline instances
      const foundPipelineInstance = pipelineInstancesSelectById(client.getServerState(), manager.pipelineInstanceId);

      expect(foundPipelineInstance).toBeDefined();

      expect(foundPipelineInstance?.executorId).toEqual(workersManager.connectionManager.connectionId);

      // Should be no segments to start
      expect(foundPipelineInstance?.segmentIds).toHaveLength(0);
   });

   describe("Config", () => {
      const pipelineManager = new PipelineManager(workersManager, {
         segments: {
            my_seg1: {
               egressPorts: ["port1"],
               ingressPorts: [],
               name: "my_seg",
               nameHash: hashName16("my_seg"),
            },
            my_seg2: {
               egressPorts: [],
               ingressPorts: ["port1"],
               name: "my_seg2",
               nameHash: hashName16("my_seg2"),
            },
         },
         manifolds: {
            port1: {
               portName: "port1",
               portHash: hashName16("port1"),
               typeId: 0,
               typeString: "int",
            },
         },
      });

      beforeEach(async () => {
         await pipelineManager.ensureRegistered();
      });

      afterEach(async () => {
         await pipelineManager.unregister();
      });

      test("Resource States", async () => {
         let pipeline_instance_state: PipelineInstance | null =
            workersManager.connectionManager.getClientState().pipelineInstances!.entities[
               pipelineManager.pipelineInstanceId
            ];

         expect(pipeline_instance_state?.state?.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Created);

         //  Update the PipelineInstance state to assign segment instances
         pipeline_instance_state = await workersManager.connectionManager.update_resource_status(
            pipelineManager.pipelineInstanceId,
            "PipelineInstances",
            ResourceActualStatus.Actual_Created
         );

         // For each manifold, set it to created
         const manifolds = await Promise.all(
            pipeline_instance_state!.manifoldIds.map(async (m) => {
               return await workersManager.connectionManager.update_resource_status(
                  m,
                  "ManifoldInstances",
                  ResourceActualStatus.Actual_Created
               )!;
            })
         );

         // Update the resource to get the assigned segments
         pipeline_instance_state = workersManager.connectionManager.getResource(
            pipelineManager.pipelineInstanceId,
            "PipelineInstances"
         );

         // For each segment, set it to created
         const segments = await Promise.all(
            pipeline_instance_state!.segmentIds.map(async (s) => {
               return await workersManager.connectionManager.update_resource_status(
                  s,
                  "SegmentInstances",
                  ResourceActualStatus.Actual_Created
               )!;
            })
         );

         // Update the resource to get the assigned segments
         pipeline_instance_state = workersManager.connectionManager.getResource(
            pipelineManager.pipelineInstanceId,
            "PipelineInstances"
         );

         expect(pipeline_instance_state?.state?.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Completed);

         // Set both the segments and the manifolds to stopped to allow the pipeline to shutdown
         await Promise.all(
            segments.map(async (s) => {
               if (!s) {
                  throw new Error("Segment should not be undefined");
               }

               return await workersManager.connectionManager.update_resource_status(
                  s.id,
                  "SegmentInstances",
                  ResourceActualStatus.Actual_Destroyed
               )!;
            })
         );

         await Promise.all(
            manifolds.map(async (m) => {
               if (!m) {
                  throw new Error("Manifold should not be undefined");
               }

               return await workersManager.connectionManager.update_resource_status(
                  m.id,
                  "ManifoldInstances",
                  ResourceActualStatus.Actual_Destroyed
               )!;
            })
         );

         //  Update the PipelineInstance state to assign segment instances
         pipeline_instance_state = await workersManager.connectionManager.update_resource_status(
            pipelineManager.pipelineInstanceId,
            "PipelineInstances",
            ResourceActualStatus.Actual_Completed
         );

         expect(pipeline_instance_state?.state?.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Stopped);

         //  Update the PipelineInstance state to assign segment instances
         pipeline_instance_state = await workersManager.connectionManager.update_resource_status(
            pipelineManager.pipelineInstanceId,
            "PipelineInstances",
            ResourceActualStatus.Actual_Stopped
         );

         expect(pipeline_instance_state?.state?.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Destroyed);

         //  Update the PipelineInstance state to assign segment instances
         pipeline_instance_state = await workersManager.connectionManager.update_resource_status(
            pipelineManager.pipelineInstanceId,
            "PipelineInstances",
            ResourceActualStatus.Actual_Destroyed
         );

         // The pipeline instance should be gone
         expect(pipeline_instance_state).toBeNull();
      });

      test("Resource State Handle Errors", async () => {
         const pipeline_instance_state: PipelineInstance | null =
            workersManager.connectionManager.getClientState().pipelineInstances!.entities[
               pipelineManager.pipelineInstanceId
            ];

         expect(pipeline_instance_state?.state?.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Created);

         // Move to completed before marking as created
         void expect(
            workersManager.connectionManager.update_resource_status(
               pipelineManager.pipelineInstanceId,
               "PipelineInstances",
               ResourceActualStatus.Actual_Completed
            )
         ).rejects.toThrow();
      });
   });
});

describe("Manifold", () => {
   const pipeline_config: IPipelineConfiguration = {
      segments: {
         my_seg1: {
            ingressPorts: [],
            egressPorts: ["port1"],
            name: "my_seg1",
            nameHash: hashName16("my_seg1"),
         },
         my_seg2: {
            ingressPorts: ["port1"],
            egressPorts: [],
            name: "my_seg2",
            nameHash: hashName16("my_seg2"),
         },
      },
      manifolds: {
         port1: {
            portName: "port1",
            portHash: hashName16("port1"),
            typeId: 1234,
            typeString: "int",
            options: {
               policy: ManifoldOptions_Policy.LoadBalance,
            },
         },
      },
   };

   describe("Second Connection", () => {
      const client: MrcTestClient = new MrcTestClient();
      const pipelineManager = PipelineManager.create(pipeline_config, ["test data"], client);
      let pipelineManager2: PipelineManager;

      beforeEach(async () => {
         await pipelineManager.ensureResourcesCreated();
         //  Update the PipelineInstance state to assign segment instances
         const pipeline_instance_state = await pipelineManager.connectionManager.update_resource_status(
            pipelineManager.pipelineInstanceId,
            "PipelineInstances",
            ResourceActualStatus.Actual_Created
         );

         // For each manifold, set it to created
         await Promise.all(
            pipeline_instance_state!.manifoldIds.map(async (s) => {
               return await pipelineManager.connectionManager.update_resource_status(
                  s,
                  "ManifoldInstances",
                  ResourceActualStatus.Actual_Created
               )!;
            })
         );

         // For each segment, set it to created
         await Promise.all(
            pipelineManager.connectionManager
               .getClientState()
               .pipelineInstances!.entities[pipelineManager.pipelineInstanceId].segmentIds.map(async (s) => {
                  return await pipelineManager.connectionManager.update_resource_status(
                     s,
                     "SegmentInstances",
                     ResourceActualStatus.Actual_Created
                  )!;
               })
         );
      });

      afterEach(async () => {
         await pipelineManager.unregister();

         if (pipelineManager2 !== undefined && pipelineManager2.isRegistered) {
            await pipelineManager2.unregister();
         }

         await client.finalizeClient();
      });

      test("Ref Counting", async () => {
         let state = pipelineManager.connectionManager.getClientState();
         expect(state.manifoldInstances!.ids).toHaveLength(1);
         expect(pipelineManager.manifoldsManager.manifoldIds).toEqual(state.manifoldInstances!.ids);

         const manifold1 = pipelineManager.manifoldsManager.manifolds[0];
         let manifold1State = manifold1.getState();

         // Verify that with a single connection, the `my_seg1` segment has a single local connection to `my_seg2`
         expect(state.segmentInstances!.ids).toHaveLength(2);
         const pipe1seg1Id: number = parseInt(state.segmentInstances!.ids[0]);
         const pipe1seg2Id: number = parseInt(state.segmentInstances!.ids[1]);

         const pipe1seg1Address: number = parseInt(state.segmentInstances?.entities[pipe1seg1Id].segmentAddress || "0");
         const pipe1seg2Address: number = parseInt(state.segmentInstances?.entities[pipe1seg2Id].segmentAddress || "1");

         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(1);
         expect(manifold1State.requestedInputSegments[pipe1seg1Address]).toBe(true);

         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(1);
         expect(manifold1State.requestedOutputSegments[pipe1seg2Address]).toBe(true);

         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(0);

         // update requested to actual
         await manifold1.syncActualSegments();

         // requested segments should be actual segments now
         state = pipelineManager.connectionManager.getClientState();
         manifold1State = manifold1.getState();
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(1);
         expect(manifold1State.requestedInputSegments[pipe1seg1Address]).toBe(true);
         expect(manifold1State.actualInputSegments).toEqual(manifold1State.requestedInputSegments);

         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(1);
         expect(manifold1State.requestedOutputSegments[pipe1seg2Address]).toBe(true);
         expect(manifold1State.actualOutputSegments).toEqual(manifold1State.requestedOutputSegments);

         // Might need to change this to a search if the order stops being deterministic
         let pipe1seg1 = state.segmentInstances!.entities[pipe1seg1Id];
         expect(pipe1seg1.name).toEqual("my_seg1");
         expect(pipe1seg1.state!.dependees.length).toEqual(1);

         let pipe1seg2 = state.segmentInstances!.entities[pipe1seg2Id];
         expect(pipe1seg2.name).toEqual("my_seg2");
         expect(pipe1seg2.state!.dependees.length).toEqual(1);

         // Now create a second connection
         pipelineManager2 = PipelineManager.create(pipeline_config, ["test data2"], client);
         await pipelineManager2.ensureResourcesCreated();

         const pipeline_instance_state2 = await pipelineManager2.connectionManager.update_resource_status(
            pipelineManager2.pipelineInstanceId,
            "PipelineInstances",
            ResourceActualStatus.Actual_Created
         );

         // For each manifold, set it to created
         await Promise.all(
            pipeline_instance_state2!.manifoldIds.map(async (s) => {
               return await pipelineManager2.connectionManager.update_resource_status(
                  s,
                  "ManifoldInstances",
                  ResourceActualStatus.Actual_Created
               )!;
            })
         );

         // For each segment, set it to created
         await Promise.all(
            pipelineManager2.connectionManager
               .getClientState()
               .pipelineInstances!.entities[pipelineManager2.pipelineInstanceId].segmentIds.map(async (s) => {
                  return await pipelineManager2.connectionManager.update_resource_status(
                     s,
                     "SegmentInstances",
                     ResourceActualStatus.Actual_Created
                  )!;
               })
         );
         // Now see what the state is, we should have 2 manifolds, 2 actual segments and 2 requested segments
         state = pipelineManager2.connectionManager.getClientState();
         expect(state.manifoldInstances!.ids).toHaveLength(2);
         expect(state.segmentInstances!.ids).toHaveLength(4);

         manifold1State = manifold1.getState();
         const manifold2 = pipelineManager2.manifoldsManager.manifolds[0];
         let manifold2State = manifold2.getState();

         const pipe2seg1Id: number = parseInt(state.segmentInstances!.ids[2]);
         const pipe2seg2Id: number = parseInt(state.segmentInstances!.ids[3]);
         const pipe2seg1Address: number = parseInt(state.segmentInstances?.entities[pipe2seg1Id].segmentAddress || "2");
         const pipe2seg2Address: number = parseInt(state.segmentInstances?.entities[pipe2seg2Id].segmentAddress || "3");

         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(1);
         expect(manifold1State.requestedInputSegments[pipe1seg1Address]).toBe(true);
         expect(manifold1State.requestedInputSegments[pipe2seg1Address]).toBe(false);
         expect(manifold1State.requestedOutputSegments[pipe1seg2Address]).toBe(true);
         expect(manifold1State.requestedOutputSegments[pipe2seg2Address]).toBe(false);

         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(2);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(0);
         expect(manifold2State.requestedInputSegments[pipe1seg1Address]).toBe(false);
         expect(manifold2State.requestedInputSegments[pipe2seg1Address]).toBe(true);
         expect(manifold2State.requestedOutputSegments[pipe1seg2Address]).toBe(false);
         expect(manifold2State.requestedOutputSegments[pipe2seg2Address]).toBe(true);

         // Both manifolds need to update their requested/actual segments
         await manifold1.syncActualSegments();
         await manifold2.syncActualSegments();

         state = pipelineManager2.connectionManager.getClientState();

         // fetch an updated version of the first manifold
         manifold1State = manifold1.getState();
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(2);
         expect(manifold1State.actualInputSegments).toEqual(manifold1State.requestedInputSegments);

         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(2);
         expect(manifold1State.actualOutputSegments).toEqual(manifold1State.requestedOutputSegments);

         manifold2State = manifold2.getState();
         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(2);
         expect(manifold2State.actualInputSegments).toEqual(manifold2State.requestedInputSegments);

         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(2);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(2);
         expect(manifold2State.actualOutputSegments).toEqual(manifold2State.requestedOutputSegments);

         pipe1seg1 = state.segmentInstances!.entities[pipe1seg1Id];
         expect(pipe1seg1.state!.dependees.length).toEqual(2);

         pipe1seg2 = state.segmentInstances!.entities[pipe1seg2Id];
         expect(pipe1seg2.state!.dependees.length).toEqual(2);

         // each segment should have refCount = 2, since 2 pipelines use them
         const pipe2seg1 = state.segmentInstances!.entities[pipe2seg1Id];
         expect(pipe2seg1.name).toEqual("my_seg1");
         expect(pipe2seg1.state!.dependees.length).toEqual(2);

         const pipe2seg2 = state.segmentInstances!.entities[pipe2seg2Id];
         expect(pipe2seg2.name).toEqual("my_seg2");
         expect(pipe2seg2.state!.dependees.length).toEqual(2);

         // Simulate a graceful shutdown of the second pipeline.
         for (const worker of pipelineManager2.workersManager.workers) {
            for (const seg of worker.segments) {
               await seg.requestSegmentStop();
               const segmentState = seg.getState();
               expect(segmentState.state!.actualStatus).toEqual(ResourceActualStatus.Actual_Stopping);
            }
         }

         // Both manifolds should have some of their requested segments removed
         manifold1State = manifold1.getState();
         manifold2State = manifold2.getState();
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(2);

         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(2);

         await manifold1.syncActualSegments();
         await manifold2.syncActualSegments();

         manifold1State = manifold1.getState();
         manifold2State = manifold2.getState();
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(1);

         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(0);

         // veirfy the refcount went down
         state = pipelineManager2.connectionManager.getClientState();
         pipe1seg1 = state.segmentInstances!.entities[pipe1seg1Id];
         expect(pipe1seg1.name).toEqual("my_seg1");
         expect(pipe1seg1.state!.dependees.length).toEqual(1);

         pipe1seg2 = state.segmentInstances!.entities[pipe1seg2Id];
         expect(pipe1seg2.name).toEqual("my_seg2");
         expect(pipe1seg2.state!.dependees.length).toEqual(1);

         for (const worker of pipelineManager2.workersManager.workers) {
            for (const seg of worker.segments) {
               await seg.sendSegmenStopped();
               const segmentState = seg.getState();
               expect(segmentState.state!.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Destroyed);
               expect(segmentState.state!.actualStatus).toEqual(ResourceActualStatus.Actual_Stopped);
            }
         }

         await manifold1.syncActualSegments();
         await manifold2.syncActualSegments();

         // Manifold2 should have been asked to shut down, manifold1 should still be running
         manifold1State = manifold1.getState();
         manifold2State = manifold2.getState();
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(1);

         // Still shouldn't be connected to anything
         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(0);

         expect(manifold1State.state!.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Completed);
         expect(manifold2State.state!.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Stopped);

         await manifold2.syncActualStatus();
         manifold1State = manifold1.getState();
         manifold2State = manifold2.getState();
         expect(manifold2State.state!.actualStatus).toEqual(ResourceActualStatus.Actual_Stopping);

         await manifold2.updateActualStatus(ResourceActualStatus.Actual_Stopped);
         manifold2State = manifold2.getState();
         expect(manifold2State.state!.actualStatus).toEqual(ResourceActualStatus.Actual_Stopped);

         await pipelineManager2.unregister();

         state = pipelineManager.connectionManager.getClientState();
         expect(state.manifoldInstances!.ids).toHaveLength(1);

         // make sure we didn't mess up the first manifold somehow
         manifold1State = manifold1.getState();
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(1);
         expect(manifold1State.actualInputSegments[pipe1seg1Address]).toBe(true);

         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(1);
         expect(manifold1State.actualOutputSegments[pipe1seg2Address]).toBe(true);
      });

      test("Shutdown second input", async () => {
         let state = pipelineManager.connectionManager.getClientState();
         const manifold1 = pipelineManager.manifoldsManager.manifolds[0];

         const pipe1seg1Id: number = parseInt(state.segmentInstances!.ids[0]);
         const pipe1seg2Id: number = parseInt(state.segmentInstances!.ids[1]);

         await manifold1.syncActualSegments();
         let manifold1State = manifold1.getState();

         // Now create a second connection
         pipelineManager2 = PipelineManager.create(pipeline_config, ["test data2"], client);
         await pipelineManager2.ensureResourcesCreated();
         const pipeline_instance_state2 = await pipelineManager2.connectionManager.update_resource_status(
            pipelineManager2.pipelineInstanceId,
            "PipelineInstances",
            ResourceActualStatus.Actual_Created
         );

         // For each manifold, set it to created
         await Promise.all(
            pipeline_instance_state2!.manifoldIds.map(async (s) => {
               return await pipelineManager2.connectionManager.update_resource_status(
                  s,
                  "ManifoldInstances",
                  ResourceActualStatus.Actual_Created
               )!;
            })
         );

         // For each segment, set it to created
         await Promise.all(
            pipelineManager2.connectionManager
               .getClientState()
               .pipelineInstances!.entities[pipelineManager2.pipelineInstanceId].segmentIds.map(async (s) => {
                  return await pipelineManager2.connectionManager.update_resource_status(
                     s,
                     "SegmentInstances",
                     ResourceActualStatus.Actual_Created
                  )!;
               })
         );
         // Now see what the state is, we should have 2 manifolds, 2 actual segments and 2 requested segments
         state = pipelineManager2.connectionManager.getClientState();
         expect(state.manifoldInstances!.ids).toHaveLength(2);
         expect(state.segmentInstances!.ids).toHaveLength(4);

         manifold1State = manifold1.getState();

         const manifold2 = pipelineManager2.manifoldsManager.manifolds[0];
         let manifold2State = manifold2.getState();

         const pipe2seg1Id: number = parseInt(state.segmentInstances!.ids[2]);
         const pipe2seg2Id: number = parseInt(state.segmentInstances!.ids[3]);

         // Both manifolds need to update their requested/actual segments
         await manifold1.syncActualSegments();
         await manifold2.syncActualSegments();

         state = pipelineManager2.connectionManager.getClientState();

         // fetch an updated version of the first manifold
         manifold1State = manifold1.getState();
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(2);
         expect(manifold1State.actualInputSegments).toEqual(manifold1State.requestedInputSegments);

         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(2);
         expect(manifold1State.actualOutputSegments).toEqual(manifold1State.requestedOutputSegments);

         manifold2State = manifold2.getState();
         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(2);
         expect(manifold2State.actualInputSegments).toEqual(manifold2State.requestedInputSegments);

         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(2);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(2);
         expect(manifold2State.actualOutputSegments).toEqual(manifold2State.requestedOutputSegments);

         // Now we need to stop the first segment in pipe2
         let foundPipe2Seg1 = false;
         for (const worker of pipelineManager2.workersManager.workers) {
            for (const seg of worker.segments) {
               if (parseInt(seg.segmentId) === pipe2seg1Id) {
                  foundPipe2Seg1 = true;
                  await seg.requestSegmentStop();
                  const segmentState = seg.getState();
                  expect(segmentState.state!.actualStatus).toEqual(ResourceActualStatus.Actual_Stopping);
               }
            }
         }

         expect(foundPipe2Seg1).toBe(true);

         // Both manifolds should have some of their reqiested segments removed
         manifold1State = manifold1.getState();
         manifold2State = manifold2.getState();
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(2);

         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(2);
         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(1);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(2);

         await manifold1.syncActualSegments();
         await manifold2.syncActualSegments();
         manifold1State = manifold1.getState();
         manifold2State = manifold2.getState();

         expect(manifold1State.state!.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Completed);
         expect(manifold2State.state!.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Completed);
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(2);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(2);

         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(1);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(1);

         // pipe2seg2 should still be running just fine consuming input from pipe1seg1
         let foundPipe2Seg2 = false;
         for (const worker of pipelineManager2.workersManager.workers) {
            for (const seg of worker.segments) {
               if (parseInt(seg.segmentId) === pipe2seg2Id) {
                  foundPipe2Seg2 = true;
                  const segmentState = seg.getState();
                  expect(segmentState.state!.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Completed);
               }
            }
         }

         expect(foundPipe2Seg2).toBe(true);

         // veirfy the refcount went down
         state = pipelineManager2.connectionManager.getClientState();
         let pipe1seg1 = state.segmentInstances!.entities[pipe1seg1Id];
         expect(pipe1seg1.name).toEqual("my_seg1");
         expect(pipe1seg1.state!.dependees.length).toEqual(2);

         let pipe1seg2 = state.segmentInstances!.entities[pipe1seg2Id];
         expect(pipe1seg2.name).toEqual("my_seg2");
         expect(pipe1seg2.state!.dependees.length).toEqual(1);

         // now stop pipe2seg2
         foundPipe2Seg2 = false;
         for (const worker of pipelineManager2.workersManager.workers) {
            for (const seg of worker.segments) {
               if (parseInt(seg.segmentId) === pipe2seg2Id) {
                  foundPipe2Seg2 = true;
                  await seg.requestSegmentStop();
               }
            }
         }

         await manifold1.syncActualSegments();
         await manifold2.syncActualSegments();

         // veirfy the refcount went down
         state = pipelineManager2.connectionManager.getClientState();
         pipe1seg1 = state.segmentInstances!.entities[pipe1seg1Id];
         expect(pipe1seg1.name).toEqual("my_seg1");
         expect(pipe1seg1.state!.dependees.length).toEqual(1);

         pipe1seg2 = state.segmentInstances!.entities[pipe1seg2Id];
         expect(pipe1seg2.name).toEqual("my_seg2");
         expect(pipe1seg2.state!.dependees.length).toEqual(1);

         // Manifold2 should have been asked to shut down, manifold1 should still be running
         manifold1State = manifold1.getState();
         manifold2State = manifold2.getState();
         expect(Object.keys(manifold1State.requestedInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualInputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.requestedOutputSegments)).toHaveLength(1);
         expect(Object.keys(manifold1State.actualOutputSegments)).toHaveLength(1);

         // Still shouldn't be connected to anything
         expect(Object.keys(manifold2State.requestedInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.actualInputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.requestedOutputSegments)).toHaveLength(0);
         expect(Object.keys(manifold2State.actualOutputSegments)).toHaveLength(0);

         expect(manifold1State.state!.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Completed);
         expect(manifold2State.state!.requestedStatus).toEqual(ResourceRequestedStatus.Requested_Stopped);

         await manifold2.updateActualStatus(ResourceActualStatus.Actual_Stopped);
         manifold2State = manifold2.getState();
         expect(manifold2State.state!.actualStatus).toEqual(ResourceActualStatus.Actual_Stopped);
      });
   });
});
