import {launchDevtoolsCli} from "@mrc/common/dev_tools";

import {ArchitectServer} from "./server";
import {setupStore} from "./store/store";

async function main()
{
   if (process.env.NODE_ENV !== "production")
   {
      // If not in production, start the redux-devtools service
      await launchDevtoolsCli("localhost", "8000");
   }

   const server = new ArchitectServer(setupStore(undefined, true));

   await server.start();

   await server.join();

   console.log("Exiting script");
}

main();
