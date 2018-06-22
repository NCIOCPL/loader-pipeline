const AbstractPipelineStep      = require('./abstract-pipeline-step');

/**
 * Class representing a loader that stores records
 */
class AbstractRecordLoader extends AbstractPipelineStep {

    /**
     * Creates a new instance of an AbstractRecordLoader
     * @param {logger} logger An instance of a logger.
     */
    constructor(logger) {

        super(logger);

        if (this.constructor === AbstractRecordLoader) {
            throw new TypeError("Cannot construct AbstractRecordSource");
        }

        if (this.loadRecord === AbstractRecordLoader.prototype.loadRecord) {
            throw new TypeError("Must implement abstract method loadRecord");
        }

        if (this.begin === AbstractRecordLoader.prototype.begin) {
            throw new TypeError("Must implement abstract method begin");
        }

        if (this.abort === AbstractRecordLoader.prototype.abort) {
            throw new TypeError("Must implement abstract method abort");
        }        
        
        if (this.end === AbstractRecordLoader.prototype.end) {
            throw new TypeError("Must implement abstract method end");
        }

    }

    /**
     * Loads a record into the data store
     */
    async loadRecord(record) {
        throw new Error("Cannot call abstract method.  Implement loadRecord in derrived class.");
    }

}

module.exports = AbstractRecordLoader;