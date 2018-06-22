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

        // As it stands, because the base constructor checks to see if
        // begin, end, and abort are implmented. super() is when that
        // happens. So this code would not be called. If this class 
        // changes, you need to add in this test.
        //if (this.constructor === AbstractRecordTransformer) {
        //    throw new TypeError("Cannot construct AbstractRecordTransformer");
        //}

        if (this.transform === AbstractRecordTransformer.prototype.transform) {
            throw new TypeError("Must implement abstract method transform");
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