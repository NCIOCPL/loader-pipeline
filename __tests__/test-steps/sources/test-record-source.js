const { AbstractRecordSource } = require('../../../index');

/**
 * This class implements a Test record source
 */
class TestRecordSource extends AbstractRecordSource {

    constructor(logger) {
        super(logger);
    }

    /**
     * Called before any records are loaded.
     */
    async begin() {
        return;
    }    

    /**
     * Get a collection of records from this source
     * @returns an array containing a single empty object
     */
    async getRecords() {
        this.logger.debug("TestRecordSource:getRecords")
        return [{}];
    }

    /**
     * Method called after all records have been loaded
     */
    async end() {
        return;
    }
    
    /**
     * Called upon a fatal loading error. Use this to clean up any items created on startup
     */
    async abort() {
        return;
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
        
        return new TestRecordSource(logger);
    }  
}

module.exports = TestRecordSource;