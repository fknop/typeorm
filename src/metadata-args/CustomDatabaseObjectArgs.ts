export interface CustomDatabaseObjectArgs {
    /**
     * Class to which table is applied.
     * Function target is a table defined in the class.
     * String target is a table defined in a json schema.
     */
    target: Function | string
}
