import { IResourceState } from "@mrc/common/entities";
import {
   ResourceActualStatus,
   ResourceDefinition,
   ResourceRequestedStatus,
} from "@mrc/proto/mrc/protos/architect_state";

export class ResourceState implements IResourceState {
   // requestedStatus: ResourceRequestedStatus = ResourceRequestedStatus.Requested_Initialized;
   // actualStatus: ResourceActualStatus = ResourceActualStatus.Actual_Unknown;
   // refCount = 0;
   // dependees = []

   private _interface: IResourceState;

   constructor(state: IResourceState) {
      this._interface = state;
   }

   public get requestedStatus(): ResourceRequestedStatus {
      return this._interface.requestedStatus;
   }

   public get actualStatus(): ResourceActualStatus {
      return this._interface.actualStatus;
   }

   public get refCount(): number {
      return this._interface.dependees.length;
   }

   public get dependees(): ResourceDefinition[] {
      return this._interface.dependees;
   }

   public get_interface() {
      return this._interface;
   }

   public static create() {
      return new ResourceState({
         requestedStatus: ResourceRequestedStatus.Requested_Initialized,
         actualStatus: ResourceActualStatus.Actual_Unknown,
         refCount: 0,
         dependees: [],
      });
   }
}
