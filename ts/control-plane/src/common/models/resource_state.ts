import { IResourceDefinition, IResourceState } from "@mrc/common/entities";
import {
   ResourceActualStatus,
   ResourceDefinition,
   ResourceRequestedStatus,
} from "@mrc/proto/mrc/protos/architect_state";

export class ResourceState implements IResourceState {
   // requestedStatus: ResourceRequestedStatus = ResourceRequestedStatus.Requested_Initialized;
   // actualStatus: ResourceActualStatus = ResourceActualStatus.Actual_Unknown;
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

   public get dependees(): ResourceDefinition[] {
      return this._interface.dependees;
   }

   public get dependers(): ResourceDefinition[] {
      return this._interface.dependers;
   }

   public get_interface() {
      return this._interface;
   }

   public static create() {
      return new ResourceState({
         requestedStatus: ResourceRequestedStatus.Requested_Initialized,
         actualStatus: ResourceActualStatus.Actual_Unknown,
         dependees: [],
         dependers: [],
      });
   }
}

export const addDependee = (dependee: IResourceDefinition, state: IResourceState): void => {
   if (
      state.dependees.some((d) => d.resourceId === dependee.resourceType && d.resourceType === dependee.resourceType)
   ) {
      throw new Error("Dependee already exists in the list");
   }
   state.dependees.push(dependee as ResourceDefinition);
};

export const removeDependee = (dependee: IResourceDefinition, state: IResourceState): void => {
   const initialLength = state.dependees.length;
   state.dependees = state.dependees.filter(
      (d) => d.resourceId !== dependee.resourceId && d.resourceType === dependee.resourceType
   );
   const finalLength = state.dependees.length;

   if (initialLength - finalLength !== 1) {
      throw new Error("Exactly one dependee should be removed");
   }
};
