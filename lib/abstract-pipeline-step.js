
/**
 * Represents a step in our ETL pipeline
 */
class AbstractPipelineStep {

    /**
     * Creates a new instance of the AbstractPipelineStep
     * @param {object} logger 
     */
    constructor(logger) {

        if (this.constructor === AbstractPipelineStep) {
            throw new TypeError("Cannot construct AbstractPipelineStep");
        }

        this.logger = logger;
    }

    /**
     * Called before any records are sourced.
     */
    async begin() {
        throw new Error("Cannot call abstract method.  Implement begin in derrived class.");
    }

    /**
     * Method called after all records have been loaded
     */
    async end() {
        throw new Error("Cannot call abstract method.  Implement end in derrived class.");
    }
    
    /**
     * Called upon a fatal loading error. Use this to clean up any items created on startup
     */
    async abort() {
        throw new Error("Cannot call abstract method.  Implement abort in derrived class.");
    }
    
    /**
     * A static method to validate a configuration object against this module type's schema
     * @param {Object} config configuration parameters to use for this instance.
     */
    static ValidateConfig(config) {
        throw new TypeError("Cannot call abstract static method ValidateConfig from derrived class");
    }    

    /**
     * A static helper function to get a configured source instance
     * @param {Object} logger the logger to use
     * @param {Object} config configuration parameters to use for this instance.
     */
    static async GetInstance(logger, config) {
        throw new TypeError("Cannot call abstract static method GetInstance from derrived class");
    }    
}

module.exports = AbstractPipelineStep;