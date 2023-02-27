import {Connection} from "@mrc/proto/mrc/protos/architect_state";
import {createSlice, PayloadAction} from "@reduxjs/toolkit";

import {createWrappedEntityAdapter} from "../../utils";

import type {RootState} from "../store";
import {addPipelineInstance, removePipelineInstance} from "./pipelineInstancesSlice";
import {addWorker, addWorkers, IWorker, removeWorker} from "./workersSlice";

export type IConnection = Omit<Connection, "$type">;

const connectionsAdapter = createWrappedEntityAdapter<IConnection>({
   selectId: (x) => x.id,
});

function workerAdded(state: ConnectionsStateType, worker: IWorker)
{
   // Handle synchronizing a new added worker
   const found_connection = connectionsAdapter.getOne(state, worker.machineId);

   if (found_connection)
   {
      found_connection.workerIds.push(worker.id);
   }
   else
   {
      throw new Error("Must add a connection before a worker!");
   }
}

export const connectionsSlice = createSlice({
   name: "connections",
   initialState: connectionsAdapter.getInitialState(),
   reducers: {
      addConnection: (state, action: PayloadAction<Pick<IConnection, "id"|"peerInfo">>) => {
         if (connectionsAdapter.getOne(state, action.payload.id))
         {
            throw new Error(`Connection with ID: ${action.payload.id} already exists`);
         }
         connectionsAdapter.addOne(state, {
            ...action.payload,
            workerIds: [],
            assignedPipelineIds: [],
         });
      },
      removeConnection: (state, action: PayloadAction<Pick<IConnection, "id">>) => {
         if (!connectionsAdapter.getOne(state, action.payload.id))
         {
            throw new Error(`Connection with ID: ${action.payload.id} not found`);
         }
         connectionsAdapter.removeOne(state, action.payload.id);
      },
   },
   extraReducers: (builder) => {
      builder.addCase(addWorker, (state, action) => {
         workerAdded(state, action.payload);
      });
      builder.addCase(addWorkers, (state, action) => {
         // Handle synchronizing a new added worker
         action.payload.forEach((p) => {
            workerAdded(state, p);
         });
      });
      builder.addCase(removeWorker, (state, action) => {
         // Handle removing a worker
         const foundConnection = connectionsAdapter.getOne(state, action.payload.machineId);

         if (foundConnection)
         {
            const index = foundConnection.workerIds.findIndex(x => x === action.payload.id);

            if (index !== -1)
            {
               foundConnection.workerIds.splice(index, 1);
            }
         }
         else
         {
            throw new Error("Must drop all workers before removing a connection");
         }
      });
      builder.addCase(addPipelineInstance, (state, action) => {
         // Handle removing a worker
         const foundConnection = connectionsAdapter.getOne(state, action.payload.machineId);

         if (foundConnection)
         {
            foundConnection.assignedPipelineIds.push(action.payload.id);
         }
         else
         {
            throw new Error("Cannot add a pipeline. Connection does not exist");
         }
      });
      builder.addCase(removePipelineInstance, (state, action) => {
         // Handle removing a worker
         const foundConnection = connectionsAdapter.getOne(state, action.payload.machineId);

         if (foundConnection)
         {
            const index = foundConnection.assignedPipelineIds.findIndex(x => x === action.payload.id);

            if (index !== -1)
            {
               foundConnection.assignedPipelineIds.splice(index, 1);
            }
         }
         else
         {
            throw new Error("Cannot remove pipeline instance, connection not found.");
         }
      });
   },
});

type ConnectionsStateType = ReturnType<typeof connectionsSlice.getInitialState>;

export const {addConnection, removeConnection} = connectionsSlice.actions;

export const {
   selectAll: connectionsSelectAll,
   selectById: connectionsSelectById,
   selectByIds: connectionsSelectByIds,
   selectEntities: connectionsSelectEntities,
   selectIds: connectionsSelectIds,
   selectTotal: connectionsSelectTotal,
} = connectionsAdapter.getSelectors((state: RootState) => state.connections);

export default connectionsSlice.reducer;
