/**
 * User land
 */
import { getMetadataArgsStorage } from "../globals"
import { EntityMetadata } from "../metadata/EntityMetadata"
import { QueryRunner } from "../query-runner/QueryRunner"
import { RdbmsSchemaBuilderHook } from "../schema-builder/custom-hooks/RdbmsSchemaBuilderHook"
import { RdbmsSchemaBuilder } from "../schema-builder/RdbmsSchemaBuilder"
import { PostgresQueryRunner } from "../driver/postgres/PostgresQueryRunner"
import { SqlInMemory } from "../driver/SqlInMemory"
import { Query } from "../driver/Query"

interface PostgresSequenceOptions {
    start?: number
    minValue?: number
    maxValue?: number
    cycle?: boolean
    increment?: number
    name: string
}

interface PostgresSequenceArgs extends PostgresSequenceOptions {
    target: string | Function
    type: "postgres:sequence"
}

interface SequenceRow {
    sequence_schema: string
    sequence_name: string
    data_type: string
    numeric_precision: number
    start_value: number
    minimum_value: number
    maximum_value: number
    increment: 1
    cycle: "YES" | "NO"
}

declare module "../metadata-args/CustomDatabaseObjectArgs" {
    type Args = PostgresSequenceArgs
    interface CustomDatabaseObjectArgs extends Args {}
}

// DECORATORS

export function PostgresSequence(
    options: PostgresSequenceOptions,
): ClassDecorator {
    return function (target) {
        getMetadataArgsStorage().customDatabaseObjects.push({
            type: "postgres:sequence",
            target,
            name: options.name,
        })
    }
}

export function createPostgresExtension({
    functions,
    sequences,
    triggers,
}: {
    functions?: Function[]
    sequences?: Function[]
    triggers?: Function[]
}): RdbmsSchemaBuilderHook {
    class PostgresExtension implements RdbmsSchemaBuilderHook {
        private existingSequences: WeakMap<
            RdbmsSchemaBuilder,
            PostgresSequenceOptions[]
        > = new WeakMap()

        private metadataTable: string = "typeorm_metadata"

        async init(
            queryRunner: PostgresQueryRunner,
            schemaBuilder: RdbmsSchemaBuilder,
            entityMetadata: EntityMetadata[],
        ): Promise<void> {
            this.metadataTable = queryRunner.getTypeormMetadataTableName()

            this.existingSequences.set(
                schemaBuilder,
                await this.getCurrentSequences(queryRunner),
            )

            return Promise.resolve()
        }

        beforeAll(
            queryRunner: PostgresQueryRunner,
            schemaBuilder: RdbmsSchemaBuilder,
            entityMetadata: EntityMetadata[],
        ): Promise<SqlInMemory> {
            const sqlInMemory = new SqlInMemory()

            const args = getMetadataArgsStorage()
            const sequencesToSync: PostgresSequenceOptions[] = []

            sequences?.forEach((sequence) => {
                const sequenceToSync =
                    args.filterCustomDatabaseObjects(sequence)
                if (
                    sequenceToSync &&
                    sequenceToSync[0].type === "postgres:sequence"
                ) {
                    sequencesToSync.push(sequenceToSync[0])
                }
            })

            const existingSequences =
                this.existingSequences.get(schemaBuilder) ?? []
            const sequenceQueries = this.getSequencesQueries(queryRunner, {
                existingSequences,
                sequencesToSync,
            })

            sqlInMemory.upQueries.push(...sequenceQueries.upQueries)
            sqlInMemory.downQueries.push(...sequenceQueries.downQueries)

            return Promise.resolve(sqlInMemory)
        }

        afterAll(
            queryRunner: QueryRunner,
            schemaBuilder: RdbmsSchemaBuilder,
            entityMetadata: EntityMetadata[],
        ): Promise<SqlInMemory> {
            return Promise.resolve({ upQueries: [], downQueries: [] })
        }

        private async getCurrentSequences(
            queryRunner: PostgresQueryRunner,
        ): Promise<PostgresSequenceOptions[]> {
            const allSequences: SequenceRow[] = await queryRunner.query(
                `SELECT * FROM "information_schema"."sequences"`,
            )

            const qb = await queryRunner.connection.createQueryBuilder()
            const metadatas: { name: string }[] = await qb
                .select()
                .from(this.metadataTable, "t")
                .where(`${qb.escape("type")} = :type`, {
                    type: "POSTGRES:SEQUENCE",
                })
                .andWhere(`${qb.escape("database")} = :database`, {
                    database: await queryRunner.getCurrentDatabase(),
                })
                .getRawMany()

            const extensionManagedSequences = metadatas.map(
                (metadata) => metadata.name,
            )

            return allSequences
                .map((sequence) => {
                    return {
                        cycle: sequence.cycle === "YES",
                        maxValue: sequence.maximum_value,
                        name: sequence.sequence_name,
                        start: sequence.start_value,
                    }
                })
                .filter((sequence) =>
                    extensionManagedSequences.includes(sequence.name),
                )
        }

        // Keep track of sequences managed by the extension
        private getSequencesQueries(
            queryRunner: PostgresQueryRunner,
            {
                existingSequences,
                sequencesToSync,
            }: {
                existingSequences: PostgresSequenceOptions[]
                sequencesToSync: PostgresSequenceOptions[]
            },
        ): SqlInMemory {
            const sqlInMemory = new SqlInMemory()

            const sequencesToDrop: PostgresSequenceOptions[] = []
            const sequencesToCreate: PostgresSequenceOptions[] = []
            const sequencesToUpdate: {
                old: PostgresSequenceOptions
                new: PostgresSequenceOptions
            }[] = []

            existingSequences.forEach((sequence) => {
                const sequenceToSync = sequencesToSync.find(
                    (sequenceToSync) => sequence.name === sequenceToSync.name,
                )
                if (!sequenceToSync) {
                    sequencesToDrop.push(sequence)
                } else if (this.isUpdatedSequence(sequence, sequenceToSync)) {
                    sequencesToUpdate.push({
                        old: sequence,
                        new: sequenceToSync,
                    })
                }
            })

            sequencesToSync.forEach((sequenceToSync) => {
                if (
                    !existingSequences.find(
                        (sequence) => sequence.name === sequenceToSync.name,
                    )
                ) {
                    sequencesToCreate.push(sequenceToSync)
                }
            })

            sequencesToDrop.forEach((sequence) => {
                sqlInMemory.upQueries.push(this.getDropSequenceQuery(sequence))
                sqlInMemory.downQueries.push(
                    this.getCreateSequenceQuery(sequence),
                )

                sqlInMemory.upQueries.push(
                    queryRunner.deleteTypeormMetadataSql({
                        type: "POSTGRES:SEQUENCE" as any,
                        name: sequence.name,
                    }),
                )
                sqlInMemory.downQueries.push(
                    queryRunner.insertTypeormMetadataSql({
                        name: sequence.name,
                        type: "POSTGRES:SEQUENCE" as any,
                    }),
                )
            })

            sequencesToCreate.forEach((sequence) => {
                sqlInMemory.upQueries.push(
                    this.getCreateSequenceQuery(sequence),
                )
                sqlInMemory.downQueries.push(
                    this.getDropSequenceQuery(sequence),
                )

                sqlInMemory.upQueries.push(
                    queryRunner.insertTypeormMetadataSql({
                        name: sequence.name,
                        type: "POSTGRES:SEQUENCE" as any,
                    }),
                )
                sqlInMemory.downQueries.push(
                    queryRunner.deleteTypeormMetadataSql({
                        type: "POSTGRES:SEQUENCE" as any,
                        name: sequence.name,
                    }),
                )
            })

            sequencesToUpdate.forEach((sequence) => {
                sqlInMemory.upQueries.push(
                    this.getAlterSequenceQuery(sequence.new),
                )
                sqlInMemory.downQueries.push(
                    this.getAlterSequenceQuery(sequence.old),
                )
            })

            return sqlInMemory
        }

        private getCreateSequenceQuery(sequence: PostgresSequenceOptions) {
            return new Query(
                `CREATE SEQUENCE "${sequence.name}"${
                    sequence.cycle ? " CYCLE" : ""
                }${sequence.start ? ` START ${sequence.start}` : ""}${
                    sequence.minValue ? ` MINVALUE ${sequence.minValue}` : ""
                }${sequence.maxValue ? ` MAXVALUE ${sequence.maxValue}` : ""}`,
            )
        }

        private getAlterSequenceQuery(sequence: PostgresSequenceOptions) {
            return new Query(
                `ALTER SEQUENCE "${sequence.name}"${
                    sequence.cycle ? " CYCLE" : ""
                }${sequence.start ? ` START ${sequence.start}` : ""}${
                    sequence.minValue ? ` MINVALUE ${sequence.minValue}` : ""
                }${sequence.maxValue ? ` MAXVALUE ${sequence.maxValue}` : ""}`,
            )
        }

        private getDropSequenceQuery(sequence: PostgresSequenceOptions) {
            return new Query(`DROP SEQUENCE IF EXISTS "${sequence.name}"`)
        }

        private isUpdatedSequence(
            a: PostgresSequenceOptions,
            b: PostgresSequenceOptions,
        ) {
            return (
                a.cycle !== b.cycle ||
                a.start !== b.start ||
                a.minValue !== b.minValue ||
                a.maxValue !== b.maxValue ||
                a.increment !== b.increment
            )
        }
    }

    return new PostgresExtension()
}
