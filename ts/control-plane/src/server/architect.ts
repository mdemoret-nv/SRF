

import {ServerDuplexStream} from "@grpc/grpc-js";
import {as, AsyncSink, merge} from "ix/asynciterable";
import {debounce} from "ix/asynciterable/operators";
import {CallContext} from "nice-grpc";
import {firstValueFrom, Subject} from "rxjs";

import {pack, packEvent, unpackEvent} from "../common/utils";
import {Any} from "../proto/google/protobuf/any";
import {
   Ack,
   ArchitectServiceImplementation,
   ClientConnectedResponse,
   ErrorCode,
   Event,
   EventType,
   PingRequest,
   PingResponse,
   PipelineRequestAssignmentRequest,
   PipelineRequestAssignmentResponse,
   RegisterWorkersRequest,
   RegisterWorkersResponse,
   ServerStreamingMethodResult,
   ShutdownRequest,
   ShutdownResponse,
   StateUpdate,
   TaggedInstance,
} from "../proto/mrc/protos/architect";
import {ControlPlaneState} from "../proto/mrc/protos/architect_state";
import {DeepPartial, messageTypeRegistry} from "../proto/typeRegistry";

import {addConnection, IConnection, removeConnection} from "./store/slices/connectionsSlice";
import {assignPipelineInstance} from "./store/slices/pipelineInstancesSlice";
import {
   activateWorkers,
   addWorkers,
   IWorker,
   removeWorker,
   workersSelectById,
   workersSelectByMachineId,
} from "./store/slices/workersSlice";
import {getRootStore, RootStore, startAction, stopAction} from "./store/store";
import {generateId} from "./utils";

interface IncomingData
{
   msg: Event, stream?: ServerDuplexStream<Event, Event>, machineId: number,
}

function unaryResponse<MessageDataT>(event: IncomingData|undefined, message_class: any, data: MessageDataT): Event
{
   // Lookup message type
   const type_registry = messageTypeRegistry.get(message_class.$type);

   const response = message_class.create(data);

   // const writer = new BufferWriter();

   // RegisterWorkersResponse.encode(response, writer);

   const any_msg = Any.create({
      typeUrl: `type.googleapis.com/${message_class.$type}`,
      value: message_class.encode(response).finish(),
   });

   return Event.create({
      event: EventType.Response,
      tag: event?.msg.tag ?? 0,
      message: any_msg,
   });
}

// function pack<MessageDataT extends UnknownMessage>(data: MessageDataT): Any {

//    // Load the type from the registry
//    const message_type = messageTypeRegistry.get(data.$type);

//    if (!message_type) {
//       throw new Error("Unknown type in type registry");
//    }

//    const any_msg = Any.create({
//       typeUrl: `type.googleapis.com/${message_type.$type}`,
//       value: message_type.encode(data).finish(),
//    });

//    return any_msg;
// }

// function unpack<MessageT extends UnknownMessage>(event: IncomingData) {
//    const message_type_str = event.msg.message?.typeUrl.split('/').pop();

//    // Load the type from the registry
//    const message_type = messageTypeRegistry.get(message_type_str ?? "");

//    if (!message_type) {
//       throw new Error(`Could not unpack message with type: ${event.msg.message?.typeUrl}`);
//    }

//    const message = message_type.decode(event.msg.message?.value as Uint8Array) as MessageT;

//    return message;
// }

// function unaryResponse<MessageDataT extends Message>(event: IncomingData, data: MessageDataT): void {

//    const any_msg = new Any();
//    any_msg.pack(data.serializeBinary(), typeUrlFromMessageClass(data) as string);

//    const message = new Event();
//    message.setEvent(EventType.RESPONSE);
//    message.setTag(event.msg.getTag());
//    message.setMessage(any_msg);

//    event.stream.write(message);
// }

class Architect implements ArchitectServiceImplementation
{
   public service: ArchitectServiceImplementation;

   private _store: RootStore;

   private shutdown_subject: Subject<void> = new Subject<void>();

   constructor(store?: RootStore)
   {
      // Use the default store if not supplied
      if (!store)
      {
         store = getRootStore();
      }

      this._store = store;

      // Have to do this. Look at
      // https://github.com/paymog/grpc_tools_node_protoc_ts/blob/master/doc/server_impl_signature.md to see about
      // getting around this restriction
      this.service = {
         eventStream: (request, context) => {
            return this.do_eventStream(request, context);
         },
         ping: async(request, context): Promise<DeepPartial<PingResponse>> => {
            return await this.do_ping(request, context);
         },
         shutdown: async(request, context): Promise<DeepPartial<ShutdownResponse>> => {
            return await this.do_shutdown(request, context);
         },
      };
   }

   public stop()
   {
      this._store.dispatch(stopAction());
   }

   public eventStream(request: AsyncIterable<Event>, context: CallContext): ServerStreamingMethodResult<{
      error?: {message?: string | undefined; code?: ErrorCode | undefined;} | undefined;
      event?: EventType | undefined;
      tag?: number | undefined;
      message?: {typeUrl?: string | undefined; value?: Uint8Array | undefined;} | undefined;
   }>
   {
      return this.do_eventStream(request, context);
   }
   public ping(request: PingRequest, context: CallContext): Promise<{tag?: number | undefined;}>
   {
      return this.do_ping(request, context);
   }
   public shutdown(request: ShutdownRequest, context: CallContext): Promise<{tag?: number | undefined;}>
   {
      return this.do_shutdown(request, context);
   }

   public onShutdownSignaled()
   {
      return firstValueFrom(this.shutdown_subject);
   }

   private async * do_eventStream(stream: AsyncIterable<Event>, context: CallContext): AsyncIterable<DeepPartial<Event>>
   {
      console.log(`Event stream created for ${context.peer}`);

      const connection: IConnection = {
         id: generateId(),
         peerInfo: context.peer,
         workerIds: [],
         assignedPipelineIds: [],
      };

      context.metadata.set("mrc-machine-id", connection.id.toString());

      const store_update_sink = new AsyncSink<Event>();

      // Subscribe to the stores next update
      const store_unsub = this._store.subscribe(() => {
         const state = this._store.getState();

         // Push out the state update
         store_update_sink.write(packEvent<ControlPlaneState>(EventType.ServerStateUpdate,
                                                              0,
                                                              ControlPlaneState.create(state as ControlPlaneState)));
      });

      // Create a new connection
      this._store.dispatch(addConnection(connection));

      // eslint-disable-next-line @typescript-eslint/no-this-alias
      const self = this;

      // Yield a connected even
      yield Event.create({
         event: EventType.ClientEventStreamConnected,
         message: pack(ClientConnectedResponse.create({
            machineId: connection.id,
         })),
      });

      const event_stream = async function*() {
         try
         {
            for await (const req of stream)
            {
               console.log(`Event stream data for ${connection.peerInfo} with message: ${req.event.toString()}`);

               yield* self.do_handle_event({
                  msg: req,
                  machineId: connection.id,
               },
                                           context);
            }
         } catch (error)
         {
            console.log(`Error occurred in stream. Error: ${error}`);
         } finally
         {
            console.log(`Event stream closed for ${connection.peerInfo}.`);

            // Input stream has completed so stop pushing events
            store_unsub();

            store_update_sink.end();
         }
      };

      try
      {
         for await (const out_event of merge(as (event_stream()), as<Event>(store_update_sink)))
         {
            yield out_event;
         }
      } catch (error)
      {
         console.log(`Error occurred in stream. Error: ${error}`);
      } finally
      {
         console.log(`All streams closed for ${connection.peerInfo}. Deleting connection.`);

         // Ensure the other streams are cleaned up
         store_unsub();

         store_update_sink.end();

         // Create a new connection
         this._store.dispatch(removeConnection(connection));
      }
   }

   private async * do_handle_event(event: IncomingData, context: CallContext)
   {
      try
      {
         switch (event.msg.event)
         {
         case EventType.ClientEventRequestStateUpdate:

            yield unaryResponse(event,
                                StateUpdate,
                                StateUpdate.create({

                                }));

            break;
         case EventType.ClientUnaryRegisterWorkers: {
            const payload = unpackEvent<RegisterWorkersRequest>(event.msg);

            const workers: IWorker[] = payload.ucxWorkerAddresses.map((value): IWorker => {
               return {
                  id: generateId(),
                  machineId: event.machineId,
                  workerAddress: value,
                  activated: false,
                  assignedSegmentIds: [],
               };
            });

            // Add the workers
            this._store.dispatch(addWorkers(workers));

            const resp = RegisterWorkersResponse.create({
               machineId: event.machineId,
               instanceIds: workersSelectByMachineId(this._store.getState(), event.machineId).map((worker) => worker.id)
            });

            yield unaryResponse(event, RegisterWorkersResponse, resp);

            break;
         }
         case EventType.ClientUnaryActivateStream: {
            const payload = unpackEvent<RegisterWorkersResponse>(event.msg);

            const workers = payload.instanceIds.map((id) => {
               const w = workersSelectById(this._store.getState(), id);

               if (!w)
               {
                  throw new Error(`Cannot activate Worker ${id}. ID does not exist`);
               }

               return w;
            });

            this._store.dispatch(activateWorkers(workers));

            yield unaryResponse(event, Ack, {});

            break;
         }
         case EventType.ClientUnaryDropWorker: {
            const payload = unpackEvent<TaggedInstance>(event.msg);

            const found_worker = workersSelectById(this._store.getState(), payload.instanceId);

            if (found_worker)
            {
               this._store.dispatch(removeWorker(found_worker));
            }

            yield unaryResponse(event, Ack, {});

            break;
         }
         case EventType.ClientUnaryRequestPipelineAssignment: {
            const payload = unpackEvent<PipelineRequestAssignmentRequest>(event.msg);

            // Check if we already have an assignment

            // Add a pipeline assignment to the machine
            const addedInstances = this._store.dispatch(assignPipelineInstance({
               ...payload,
               machineId: event.machineId,
            }));

            yield unaryResponse(event,
                                PipelineRequestAssignmentResponse,
                                PipelineRequestAssignmentResponse.create(addedInstances));

            break;
         }
         default:
            break;
         }
      } catch (error)
      {
         console.log(`Error occurred handing event. Error: ${error}`);
      }
   }

   private async do_shutdown(req: ShutdownRequest, context: CallContext): Promise<DeepPartial<ShutdownResponse>>
   {
      console.log(`Issuing shutdown promise from ${context.peer}`);

      // Signal that shutdown was requested
      this.shutdown_subject.next();

      return ShutdownResponse.create();
   }

   private async do_ping(req: PingRequest, context: CallContext): Promise<DeepPartial<PingResponse>>
   {
      console.log(`Ping from ${context.peer}`);

      return PingResponse.create({
         tag: req.tag,
      });
   }
}

// class Architect implements ArchitectServer{
//    [name: string]: UntypedHandleCall;
//    eventStream: handleBidiStreamingCall<Event, Event>;
//    shutdown: handleUnaryCall<ShutdownRequest, ShutdownResponse>;

// }

// class Architect implements ArchitectServer {
//    [name: string]: UntypedHandleCall;

//    constructor() {
//       console.log("Created");

//       this.a = 5;
//    }

//    public eventStream(call: ServerDuplexStream<Event, Event>): void {
//       console.log(`Event stream created for ${call.getPeer()}`);

//       call.on("data", (req: Event) => {
//          console.log(`Event stream data for ${call.getPeer()} with message: ${req.event.toString()}`);
//          // this.do_handle_event(req, call);
//       });

//       call.on("error", (err: Error) => {
//          console.log(`Event stream errored for ${call.getPeer()} with message: ${err.message}`);
//       });

//       call.on("end", () => {
//          console.log(`Event stream closed for ${call.getPeer()}`);
//       });
//    }

//    private do_handle_event(event: Event, call: ServerDuplexStream<Event, Event>): void{

//       try {
//          switch (event.event) {
//             case EventType.ClientEventRequestStateUpdate:

//                break;

//             default:
//                break;
//          }
//       } catch (error) {

//       }

//    }

// }

export {
   Architect,
};