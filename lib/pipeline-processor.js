
const AbstractRecordSource      = require('./abstract-record-source');
const AbstractRecordTransformer = require('./abstract-record-transformer');
const AbstractRecordLoader      = require('./abstract-record-loader');


/**
 * Represents the main pipeline ETL processor.
 */
class PipelineProcessor {
    
    /**
     * Creates a new instance of the PipelineProcessor
     * @param {logger} logger An instance of a logger.
     * @param {Object} param1 The configuration for this pipeline processor
     * @param {Object} param1.source The configuration for the source pipeline step
     * @param {Array}  param1.transformers The configuration for the transformer pipeline steps
     * @param {Object} param1.loader The configuration for the loader pipeline step
     */
    constructor(logger, { source = false, transformers = [], loader = false } = {}) {
        
        if (!this.isBasicConfigStructValid(source)) {
            throw new Error("The source configuration is not valid");
        }

        //Test all the transformer configs
        if (!Array.isArray(transformers)) {
            throw new Error("The transformers configuration is not valid");
        }

        if (!transformers.map(this.isBasicConfigStructValid.bind(this)).reduce((a,c) => a && c, true)) {
            throw new Error("The transformers configuration is not valid");
        }

        if (!this.isBasicConfigStructValid(loader)) {
            throw new Error("The loader configuration is not valid");
        }
        
        this.logger = logger;
        this.config = {
            source: source,
            transformers: transformers,
            loader: loader
        };

        this.sourceStep = false;
        this.transformerSteps = [];
        this.loaderStep = false;

        this.recordsProcessed = 0;
        this.recordsFetched = 0;
    }

    /**
     * Validate the basic configuration structure
     * @param {*} config The configuration to test
     */
    isBasicConfigStructValid(config) {
        if (!config || typeof config !== 'object') {
            return false;
        }

        if (!config['module'] || (typeof config['module'] !== 'string' && typeof config['module'] !== 'function')){
            return false;
        }

        if (!config['config'] || typeof config['config'] !== 'object'){
            return false;
        }

        return true;
    }

    /**
     * Loads a pipeline step and gets an instance of the module, configured by the config.
     * @param {function} expectedClass The expected class type of step module (this should be the class and not a string)
     * @param {string|function} module The path to the step module, or the loaded module class
     * @param {object} stepConfig The configuration for this step
     */
    async loadPipelineStep(expectedClass, stepmodule, stepConfig) {

        if (typeof expectedClass !== 'function') {
            throw new Error('ExpectedClass needs to be a base class for the module that is being loaded');
        }

        let loadedModule;
        let moduleName;

        if (typeof stepmodule === 'string') {
            moduleName = stepmodule;
            // Load the module
            try {
                loadedModule = require(stepmodule);
            } catch(err) {
                this.logger.error(`Could not load step, ${moduleName}.`);
                this.logger.error(err);
                throw err;
            }
        } else if (typeof stepmodule === 'function') {
            moduleName = stepmodule.name;
            loadedModule = stepmodule;
        } else {
            throw new Error('Invalid type for module parameter');
        }

        //Now let's test the type of the module to ensure 
        //that it derives from the expected class.
        if (!expectedClass.isPrototypeOf(loadedModule)) {
            const expectedType = expectedClass.name;
            throw new Error(`${moduleName} does not match expected type of ${expectedType}`);
        }

        let configErrors = [];

        // Test the config
        try {
            configErrors = loadedModule.ValidateConfig(stepConfig);
        } catch(err) {
            this.logger.error(`Module ${moduleName} Configuration Errors Detected`);
            this.logger.error(err);
            throw new Error("Invalid Configuration")
        }

        if (configErrors.length > 0) {
            this.logger.error(`Module ${moduleName} Configuration Errors Detected`);
            configErrors.forEach(err => {
              this.logger.error(err);
            });  
            throw new Error("Invalid Configuration")
        }         

        // Get an instance of the module
        let moduleInstance;

        try {
            moduleInstance = await loadedModule.GetInstance(this.logger, stepConfig);
        } catch (err) {
            this.logger.error(`Could not create instance of step, ${moduleName}.`);
            this.logger.error(err);
            throw err;
        }

        return moduleInstance;
    }

    /**
     * Setup the steps in the pipeline
     */
    async loadPipeline() {        

        // Loading up a step is an asynchronous task. We want to load all of the steps as
        // asynchronously as possible. So we will need to create an array of promises to 
        // run Promise.all on.
        let stepsToLoad = [];

        //Load the source
        stepsToLoad.push((async () => {
            this.sourceStep = await this.loadPipelineStep(AbstractRecordSource, this.config.source.module, this.config.source.config);
        })());

        //Load the transformers        
        stepsToLoad.push(...this.config.transformers.map(async tc => {
            this.transformerSteps.push(await this.loadPipelineStep(AbstractRecordTransformer, tc.module, tc.config));
        }));        

        //Load the loader
        stepsToLoad.push((async () => {
            this.loaderStep = await this.loadPipelineStep(AbstractRecordLoader, this.config.loader.module, this.config.loader.config);
        })());

        // Now run all the steps to load
        await Promise.all(stepsToLoad);
    }

    /**
     * Gets all the steps of the pipeline
     */
    getSteps() {
        return [
            this.sourceStep,
            ...this.transformerSteps,
            this.loaderStep
        ];  
    }

    /**
     * Internal method to run the transform and loaders steps on a record
     * @param {*} record The record to process
     */
    async processRecord(record) {

        let currObj = record;

        //Steps of the pipeline must be chained and be sequential.

        // Apply all the transformers
        for (let transformerStep of this.transformerSteps) {
            currObj = await transformerStep.transform(currObj);
        }

        //Load the object
        await this.loaderStep.loadRecord(currObj);

        this.recordsProcessed++;
    }

    /**
     * Runs the loading process.
     */
    async run() {
        this.logger.info("Beginning Pipeline")

        // Phase 1. Load all the steps of the pipeline
        await this.loadPipeline();

        // Phase 2. Call Begins to Setup Steps
        try {
            this.logger.info("Calling Begin()");
            await Promise.all(this.getSteps().map(step => step.begin()));            
        } catch (err) {
            this.logger.error("Could not initialize steps...");

            //Abort processes
            try {
                this.logger.error("Aborting...");
                await Promise.all(this.getSteps().map(step => step.abort()));
            } catch(abortErr) {
                this.logger.error("Abort failed")
                this.logger.error(`Abort Error - ${abortErr}`);
            }            

            throw err;
        }


        try {
            // Phase 3a. Generate the docs to process
            this.logger.info("Fetching records from source");
            const records = await this.sourceStep.getRecords();
            this.recordsFetched = records.length;

            // Phase 3b. Push docs to pipeline
            this.logger.info("Transforming & storing records from source");
            const pipelinedRecords = records.map(async (record) => {
                await this.processRecord(record);

                //Status message here would be nice...
                if (this.recordsProcessed % 10 === 0) {
                    this.logger.info(`${this.recordsProcessed} records processed`)
                }
            })
            await Promise.all(pipelinedRecords);

        } catch (err) {
            this.logger.error("Could not process pipeline...");

            //Abort processes
            try {
                this.logger.error("Aborting...");
                await Promise.all(this.getSteps().map(step => step.abort()));
            } catch(abortErr) {
                this.logger.error("Abort failed")
                this.logger.error(`Abort Error - ${abortErr}`);
            }

            throw err;
        }

        // Phase 4. Call Begins to Setup Steps
        try {
            this.logger.info("Calling end()");
            await Promise.all(this.getSteps().map(step => step.end()));
        } catch (err) {
            this.logger.error("Could not end steps...");

            //Abort processes
            try {
                this.logger.error("Aborting...");
                await Promise.all(this.getSteps().map(step => step.abort()));
            } catch(abortErr) {
                this.logger.error("Abort failed")
                this.logger.error(`Abort Error - ${abortErr}`);
            }            

            throw err;
        }


        this.logger.info(`Pipeline Complete: ${this.recordsProcessed} records processed`);
    }
}

module.exports = PipelineProcessor;