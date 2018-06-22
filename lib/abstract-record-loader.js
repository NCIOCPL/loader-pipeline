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

        // As it stands, because the base constructor checks to see if
        // begin, end, and abort are implmented. super() is when that
        // happens. So this code would not be called. If this class 
        // changes, you need to add in this test.
        //if (this.constructor === AbstractRecordLoader) {
        //    throw new TypeError("Cannot construct AbstractRecordLoader");
        //}

        if (this.loadRecord === AbstractRecordLoader.prototype.loadRecord) {
            throw new TypeError("Must implement abstract method loadRecord");
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