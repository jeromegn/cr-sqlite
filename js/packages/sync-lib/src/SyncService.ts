import OutboundStream from "./OutboundStream";
import {
  AckChangesMsg,
  ApplyChangesMsg,
  Change,
  Config,
  EstablishStreamMsg,
  GetChangesMsg,
} from "./Types";

export default class SyncService {
  constructor(public readonly config: Config) {}

  /**
   * Will create the database if it does not exist and apply the schema.
   * If the database does exist, it will migrate the schema.
   * If there was no schema update this is a no-op.
   *
   * If we need to migrate the DB, any streaming connections to it
   * are closed.
   *
   * Any new connections are refused until the migration completes.
   *
   * @param dbid
   * @param schema
   */
  createOrMigrateDatabase(dbid: string, schemaName: string) {}

  /**
   * Upload a new schema to the server.
   * You should have auth checks around this.
   *
   * Migrations are done lazily as database connections are opened.
   *
   * The server will retain all copies of the schema for debugging purposes.
   * You can remove old copies of the schema through the `listSchemas` and
   * `deleteSchema` methods.
   */
  uploadSchema(
    schemaName: string,
    schemaContents: string,
    schemaVersion: string
  ) {
    throw new Error();
  }

  listSchemas(): string[] {
    return [];
  }

  applyChanges(msg: ApplyChangesMsg): void {}

  /**
   * Clients should only ever have 1 outstanding `getChanges` request to the same DBID at a time.
   * If a client issues a getChanges request to the same DB while they have one in-flight,
   * they should ignore the response to the first request.
   * @param msg
   * @returns
   */
  getChanges(msg: GetChangesMsg): Change[] {
    return [];
  }

  /**
   * Start streaming changes from the server to the client
   * such that the client does not have to issue a request
   * for changes.
   */
  startOutboundStream(msg: EstablishStreamMsg): OutboundStream {
    return new OutboundStream();
  }
}