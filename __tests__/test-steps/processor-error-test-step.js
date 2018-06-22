const AbstractPipelineStep = require('../../lib/abstract-pipeline-step');

class ProcessorErrorTestStep extends AbstractPipelineStep {
  /**
   * Creates a new instance of the AbstractPipelineStep
   * @param {object} logger 
   */
  constructor(logger) {
    super(logger);    
  }
  
  async begin() {}
  async end() {}
  async abort() {}

  /**
   * A static method to validate a configuration object against this module type's schema
   * @param {Object} config configuration parameters to use for this instance.
   */
  static ValidateConfig({ fail = false, errors=false } = {}) {
    if (fail) {
      if (errors === false) {
        throw new Error("Could not validate");
      } else {
        return errors;
      }
    } else {
      return [];
    }
  }    

  /**
   * A static helper function to get a configured source instance
   * @param {Object} logger the logger to use
   * @param {Object} config configuration parameters to use for this instance.
   */
  static async GetInstance(logger, { fail = false, errors=[]}) {
    if (fail) {
      throw new Error("Could not get instance");
    } else {
      return new ProcessorErrorTestStep(logger);
    }
  }
}

module.exports = ProcessorErrorTestStep;