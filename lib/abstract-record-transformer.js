const AbstractPipelineStep      = require('./abstract-pipeline-step');

/**
 * Class representing a transformer that converts a raw record from the source to one for loading
 */
class AbstractRecordTransformer extends AbstractPipelineStep {

    /**
     * Creates a new instance of an AbstractRecordTransformer
     * @param {logger} logger An instance of a logger.
     */
    constructor(logger) {

        super(logger);

        if (this.constructor === AbstractRecordTransformer) {
            throw new TypeError("Cannot construct AbstractRecordTransformer");
        }

        if (this.transform === AbstractRecordTransformer.prototype.transform) {
            throw new TypeError("Must implement abstract method transform");
        }

        if (this.begin === AbstractRecordTransformer.prototype.begin) {
            throw new TypeError("Must implement abstract method begin");
        }

        if (this.end === AbstractRecordTransformer.prototype.end) {
            throw new TypeError("Must implement abstract method end");
        }
    }

    /**
     * Transforms the record 
     * @param {Object} data the object to be transformed
     * @returns the transformed object
     */
    async transform(data) {
        throw new Error("Cannot call abstract method.  Implement transform in derived class.");
    }

}

module.exports = AbstractRecordTransformer;