const { AbstractRecordTransformer } = require('../../../index');

/**
 * This class implements a Test Record transformer
 */
class TestRecordTransformer extends AbstractRecordTransformer {

    constructor(logger) {
        super(logger);
    }

    /**
     * Called before any records are transformed -- load mappers and anything else here.
     */
    async begin() {
        this.logger.debug("TestRecordTransformer:begin");
    }

    /**
     * Transforms the record 
     */
    async transform(data) {
        this.logger.debug("TestRecordTransformer:transform");
        return {};
    }

    /**
     * Called if error has occurred to allow for clean exit.
     */
    async abort() {
        return;
    }

    /**
     * Method called after all records have been transformed
     */
    async end() {
        this.logger.debug("TestRecordTransformer:end");
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
        return new TestRecordTransformer(logger);
    } 
}

module.exports = TestRecordTransformer;