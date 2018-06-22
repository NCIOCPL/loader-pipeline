const { AbstractRecordLoader } = require('../../../index');

/**
 * This class implements a test Record loader
 */
class TestRecordLoader extends AbstractRecordLoader {

    constructor(logger) {
        super(logger);
    }

    /**
     * Called before any records are fetched
     */
    async begin() {
        this.logger.debug("TestRecordLoader:begin")
    }

    /**
     * Loads a record into the data store
     */
    async loadRecord(record) {
        this.logger.debug("TestRecordLoader:loadRecord")
        console.log(record)
    }

    /**
     * Called upon a fatal loading error. Use this to clean up any items created on startup
     */
    async abort() {
        this.logger("TestRecordLoader:abort")
    }

    /**
     * Method called after all records have been loaded
     */
    async end() {
        throw new Error("Cannot call abstract method.  Implement end in derived class.");
    }    

    /**
     * A static method to validate a configuration object against this module type's schema
     * @param {Object} config configuration parameters to use for this instance.
     */
    static ValidateConfig(config) {
        return [];
    }    

    /**
     * A static helper function to get a configured source instance
     * @param {Object} logger the logger to use
     * @param {Object} config configuration parameters to use for this instance.
     */
    static async GetInstance(logger, config) {
        return new TestRecordLoader(logger);
    }  
}

module.exports = TestRecordLoader;