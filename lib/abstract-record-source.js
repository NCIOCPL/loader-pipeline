const AbstractPipelineStep      = require('./abstract-pipeline-step');

/**
 * Class representing a source of records
 */
class AbstractRecordSource extends AbstractPipelineStep{

    /**
     * Creates a new instance of 
     * @param {logger} logger An instance of a logger.
     */
    constructor(logger) {
        super(logger);

        if (this.constructor === AbstractRecordSource) {
            throw new TypeError("Cannot construct AbstractRecordSource");
        }

        if (this.getRecords === AbstractRecordSource.prototype.getRecords) {
            throw new TypeError("Must implement abstract method getRecords");
        }
    }

    /**
     * Get a collection of records from this source
     */
    async getRecords() {
        throw new Error("Cannot call abstract method.  Implement getRecords in derrived class.");
    }
}

module.exports = AbstractRecordSource;