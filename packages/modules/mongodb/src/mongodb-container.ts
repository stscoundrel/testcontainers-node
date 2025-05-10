import { AbstractStartedContainer, ExecResult, GenericContainer, StartedTestContainer, Wait } from "testcontainers";

const MONGODB_PORT = 27017;

export class MongoDBContainer extends GenericContainer {
  private username: string | null = null;
  private password: string | null = null;

  constructor(image = "mongo:4.0.1") {
    super(image);
    this.withExposedPorts(MONGODB_PORT)
      .withCommand(["--replSet", "rs0"])
      .withWaitStrategy(Wait.forLogMessage(/.*waiting for connections.*/i))
      .withStartupTimeout(120_000);
  }

  public withUsername(username: string): this {
    this.username = username;
    return this;
  }

  public withPassword(rootPassword: string): this {
    this.password = rootPassword;
    return this;
  }

  public override async start(): Promise<StartedMongoDBContainer> {
    if (this.username && this.password) {
      const containerKeyfilePath = "/tmp/mongo-keyfile";
      this.withCommand([
        "/bin/sh",
        "-c",
        `
        openssl rand -base64 756 > ${containerKeyfilePath} &&
        chmod 600 ${containerKeyfilePath} &&
        chown mongodb:mongodb ${containerKeyfilePath} &&
        exec mongod --replSet rs0 --keyFile ${containerKeyfilePath} --bind_ip_all
        `,
      ]);
      this.withEnvironment({ MONGO_INITDB_ROOT_USERNAME: this.username, MONGO_INITDB_ROOT_PASSWORD: this.password });
    }

    return new StartedMongoDBContainer(await super.start(), this.username, this.password);
  }

  protected override async containerStarted(startedTestContainer: StartedTestContainer): Promise<void> {
    await this.initReplicaSet(startedTestContainer);
  }

  private async initReplicaSet(startedTestContainer: StartedTestContainer) {
    await this.executeMongoEvalCommand(startedTestContainer, `rs.initiate();`);
    await this.executeMongoEvalCommand(startedTestContainer, this.buildMongoWaitCommand());
  }

  private async executeMongoEvalCommand(startedTestContainer: StartedTestContainer, command: string) {
    const execResult = await startedTestContainer.exec(this.buildMongoEvalCommand(command));
    this.checkMongoNodeExitCode(execResult);
  }

  private buildMongoEvalCommand(command: string) {
    const cmd = [this.getMongoCmdBasedOnImageTag()];

    if (this.username && this.password) {
      cmd.push("--username", this.username, "--password", this.password, "--authenticationDatabase", "admin");
    }

    cmd.push("--eval", command);

    return cmd;
  }

  private getMongoCmdBasedOnImageTag() {
    return parseInt(this.imageName.tag[0]) >= 5 ? "mongosh" : "mongo";
  }

  private checkMongoNodeExitCode(execResult: ExecResult) {
    const { exitCode, output } = execResult;
    if (execResult.exitCode !== 0) {
      throw new Error(`Error running mongo command. Exit code ${exitCode}: ${output}`);
    }
  }

  private buildMongoWaitCommand() {
    return `
    var attempt = 0;
    while(db.runCommand({isMaster: 1}).ismaster==false) {
      if (attempt > 60) {
        quit(1);
      }
      print(attempt); sleep(100); attempt++;
    }
    `;
  }
}

export class StartedMongoDBContainer extends AbstractStartedContainer {
  constructor(
    startedTestContainer: StartedTestContainer,
    private readonly username: string | null,
    private readonly password: string | null
  ) {
    super(startedTestContainer);
  }

  public getConnectionString(): string {
    if (this.username !== null && this.password !== null) {
      return `mongodb://${this.username}:${this.password}@${this.getHost()}:${this.getMappedPort(MONGODB_PORT)}`;
    }

    return `mongodb://${this.getHost()}:${this.getMappedPort(MONGODB_PORT)}`;
  }
}
