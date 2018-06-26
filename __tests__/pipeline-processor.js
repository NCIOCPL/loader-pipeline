const winston               = require('winston');
const WinstonNullTransport  = require('winston-null-transport');
const path                  = require('path');

const { 
    PipelineProcessor, AbstractRecordTransformer, AbstractRecordLoader, AbstractRecordSource 
}   = require('../index');
const AbstractPipelineStep    = require('../lib/abstract-pipeline-step');

const TestRecordSource        = require('./test-steps/sources/test-record-source');
const TestRecordTransformer   = require('./test-steps/transformers/test-record-transformer');
const TestRecordLoader        = require('./test-steps/loaders/test-record-loader');

const logger = winston.createLogger({
    level: 'debug',
    format: winston.format.simple(),
    transports: [
        new WinstonNullTransport()
    ]
})

describe('PipelineProcessor', async () => {


    describe('Constructor Error Handling', () => {

        const goodStepInfo = Object.freeze({
            module: "string", 
            config: {}
        });
        
        const goodConfig = Object.freeze({
            source: goodStepInfo,
            transformers: [
                goodStepInfo
            ],
            loader: goodStepInfo
        });

        const badStepInfo = Object.freeze({
            module: 5, 
            config: "string" 
        });

        ////////////
        /// SEARCHPATHS
        ///////////
        it('throws on searchPaths is junk', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, searchPaths: "" });
            }).toThrow("searchPaths must be an array");
        });

        ////////////
        /// SOURCE
        ///////////
        it('throws on source is null', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, source: null });
            }).toThrow("The source configuration is not valid");
        });

        it('throws on source is junk', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, source: badStepInfo });
            }).toThrow("The source configuration is not valid");
        });

        ////////////
        // Loader
        ////////////
        it('throws on loader is null', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, loader: null});
            }).toThrow("The loader configuration is not valid");
        });

        it('throws on source is junk', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, loader: badStepInfo});
            }).toThrow("The loader configuration is not valid");
        });

        ///////////////
        // Transformers
        ///////////////
        it('throws on transformer is null', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, transformers: null});
            }).toThrow("The transformers configuration is not valid");
        });

        it('throws on transformer is not array', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, transformers: "chicken"});
            }).toThrow("The transformers configuration is not valid");
        });

        it('throws on a single transformer is junk', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, transformers: [ badStepInfo ]});
            }).toThrow("The transformers configuration is not valid");
        });

        it('throws on a transformer is junk with good ones', () => {
            expect(() => {
                new PipelineProcessor(null, {...goodConfig, transformers: [ goodStepInfo, badStepInfo ]});
            }).toThrow("The transformers configuration is not valid");
        });    
    })

    describe('isBasicConfigStructValid', () => {

        const goodStepInfo = Object.freeze({
            module: "string", 
            config: {}
        });
        
        const goodConfig = Object.freeze({
            source: goodStepInfo,
            transformers: [
                goodStepInfo
            ],
            loader: goodStepInfo
        });

        const processor = new PipelineProcessor(
            logger,
            goodConfig    
        )

        it('false on no config', () => {
            const actual = processor.isBasicConfigStructValid();
            expect(actual).not.toBeTruthy();
        })

        it('false on bad config type', () => {
            const actual = processor.isBasicConfigStructValid("chicken");
            expect(actual).not.toBeTruthy();
        })

        it('false on no module', () => {
            const actual = processor.isBasicConfigStructValid({});
            expect(actual).not.toBeTruthy();
        })

        it('false on module not valid', () => {
            const actual = processor.isBasicConfigStructValid({module: 5});
            expect(actual).not.toBeTruthy();
        })

        it('false on no step config', () => {
            const actual = processor.isBasicConfigStructValid({module: "a"});
            expect(actual).not.toBeTruthy();
        })

        it('false on step config not valid type', () => {
            const actual = processor.isBasicConfigStructValid({
                module: "a",
                config: "chicken"
            });
            expect(actual).not.toBeTruthy();
        })

        it('Handles module as string', () => {
            const actual = processor.isBasicConfigStructValid({
                module: "a",
                config: {}
            });
            expect(actual).toBeTruthy();
        })

        it('Handles module as Class', () => {
            const actual = processor.isBasicConfigStructValid({
                module: TestRecordLoader,
                config: {}
            });
            expect(actual).toBeTruthy();
        })
    });

    describe('loadPipelineStep', async () => {
        const PATH_TO_MODULE = path.join(__dirname, 'test-steps', 'processor-error-test-step')

        const goodStepInfo = Object.freeze({
            module: "string", 
            config: {}
        });
        
        const goodConfig = Object.freeze({
            source: goodStepInfo,
            transformers: [
                goodStepInfo
            ],
            loader: goodStepInfo
        });
        
        ////////////////////////
        /// Everything worked ok
        ////////////////////////

        
        it('loads a string step', async() => {
            const processor = new PipelineProcessor(logger, goodConfig);

            let actual = await processor.loadPipelineStep(AbstractPipelineStep,PATH_TO_MODULE, {});
            expect(actual).toBeTruthy();
        });

        it('loads a string step using custom search path', async() => {
            const searchPath = path.join(__dirname, 'test-steps');
            const stepPath = "./loaders/test-record-loader";
            const processor = new PipelineProcessor(logger, {
                ...goodConfig,
                loader: {
                    module: stepPath, 
                    config: {}
                },
                searchPaths: [searchPath]
            });

            let actual = await processor.loadPipelineStep(AbstractRecordLoader, stepPath, {});
            expect(actual).toBeTruthy();
        });

        it('loads a module step', async() => {
            const processor = new PipelineProcessor(logger, goodConfig);

            const ProcessorErrorTestStep = require(PATH_TO_MODULE);
            let actual = await processor.loadPipelineStep(AbstractPipelineStep,ProcessorErrorTestStep, {});
            expect(actual).toBeTruthy();
        });
        
        

        //////////////////////
        /// Error Cases
        //////////////////////
        describe('throws on', async() => {
            const processor = new PipelineProcessor(logger, goodConfig);

            it('expected class is junk', async () => {
    
                expect.assertions(1);
                try {
                     await processor.loadPipelineStep("foo");
                } catch (err) {
                    expect(err).toMatchObject({
                        message: "ExpectedClass needs to be a base class for the module that is being loaded"
                    });
                }    
            });

            it('module is junk', async () => {
    
                expect.assertions(1);
                try {
                     await processor.loadPipelineStep(AbstractPipelineStep, 5, {});
                } catch (err) {
                    expect(err).toMatchObject({
                        message: "Invalid type for module parameter"
                    });
                }    
            });

            it('inability to load the module', async () => {
    
                expect.assertions(1);
                try {
                     await processor.loadPipelineStep(AbstractPipelineStep,'/path/does/not/exist', {});
                } catch (err) {
                    expect(err).toMatchObject({
                        message: "Could not load step, /path/does/not/exist."
                    });
                }    
            });

            it('module not of type', async () => {
    
                expect.assertions(1);
                try {
                     await processor.loadPipelineStep(AbstractRecordTransformer,TestRecordSource, {});
                } catch (err) {
                    expect(err).toMatchObject({
                        message: "TestRecordSource does not match expected type of AbstractRecordTransformer"
                    });
                }    
            });


            it('invalid step configuration by exception', async () => {
                expect.assertions(1);
                try {
                    await processor.loadPipelineStep(
                        AbstractPipelineStep,
                        PATH_TO_MODULE, 
                        { fail: true }
                    );
                } catch (err) {
                     expect(err).toMatchObject({
                         message: "Invalid Configuration"
                     });
                }
            });
    
            it('invalid step configuration with errors', async () => {
    
                // TODO: We should check the logger to see if messages are written
    
                expect.assertions(1);
                try {
                    await processor.loadPipelineStep(
                        AbstractPipelineStep,
                        PATH_TO_MODULE, 
                        { 
                            fail: true,
                            errors: [ new Error("Test Error")]
                        }
                    );
                } catch (err) {
                     expect(err).toMatchObject({
                         message: "Invalid Configuration"
                     });
                }
            });        
        
            it('inability to get an instance of the step', async () => {
                expect.assertions(1);
                try {
                    await processor.loadPipelineStep(
                        AbstractPipelineStep,
                        PATH_TO_MODULE, 
                        { 
                            fail: true,
                            errors: []
                        }
                    );
                } catch (err) {
                     expect(err).toMatchObject({
                         message: "Could not get instance"
                     });
                }            
            });    
        })
        
    });

    describe('loadPipeline', async () => {        

        const goodConfig = {
            "source": {
                "module": path.join(__dirname, "test-steps", "sources", "test-record-source"),
                "config": {}
            },
            "transformers": [
                {
                    "module": path.join(__dirname, "test-steps", "transformers", "test-record-transformer"),
                    "config": {}
                }
            ],
            "loader": {
                "module": path.join(__dirname, "test-steps", "loaders", "test-record-loader"),
                "config": {}
            }
        };
        
        it('loads', async () => {
            const processor = new PipelineProcessor(
                logger,
                goodConfig    
            )

            await processor.loadPipeline();
    
            expect(processor.sourceStep).toBeInstanceOf(TestRecordSource);
            expect(processor.transformerSteps).toHaveLength(1);
            expect(processor.transformerSteps[0]).toBeInstanceOf(TestRecordTransformer);
            expect(processor.loaderStep).toBeInstanceOf(TestRecordLoader);
        });

        it('has incorrect source type', async() => {
            const processor = new PipelineProcessor(
                logger,
                { ...goodConfig, source: { module: TestRecordLoader, config: {} } }
            );
            
            expect.assertions(1);
            try {
                await processor.loadPipeline();
            } catch (err) {
                expect(err).toMatchObject({
                    message: "TestRecordLoader does not match expected type of AbstractRecordSource"
                });
            }            
        });

        it('has incorrect loader type', async() => {
            const processor = new PipelineProcessor(
                logger,
                { ...goodConfig, loader: { module: TestRecordSource, config: {} } }
            );
            
            expect.assertions(1);
            try {
                await processor.loadPipeline();
            } catch (err) {
                expect(err).toMatchObject({
                    message: "TestRecordSource does not match expected type of AbstractRecordLoader"
                });
            }            
        });

        it('has incorrect transformer type', async() => {
            const processor = new PipelineProcessor(
                logger,
                { ...goodConfig, transformers: [{ module: TestRecordSource, config: {} }] }
            );
            
            expect.assertions(1);
            try {
                await processor.loadPipeline();
            } catch (err) {
                expect(err).toMatchObject({
                    message: "TestRecordSource does not match expected type of AbstractRecordTransformer"
                });
            }            
        });

        it('encounters an error', async () => {
            const processor = new PipelineProcessor(
                logger,
                { ...goodConfig, source: { module: "badpath", config: {} } }
            );
            
            expect.assertions(1);
            try {
                await processor.loadPipeline();
            } catch (err) {
                expect(err).toMatchObject({
                    message: "Could not load step, badpath."
                });
            }
        });
    })

    function SetupSpiedProcess(sourceData, errorInXfrm = false, errorInEnd = false, errorInAbort = false) {

            //Spied Counts
            const sc = {
                source: { b:0, gr:0, a:0, e:0 },
                transformer: { b:0, t:0, a:0, e:0 },
                loader: { b:0, lr:0, a:0, e:0 }
            }

            const d = {
                src: sourceData,
                xfrm: [],
                ldr: []
            }

            class SpiedRecordSource extends AbstractRecordSource {
                constructor(logger) { super(logger); }
                async begin() { sc.source.b++; }    
                async getRecords() { sc.source.gr++; return d.src; }
                async end() { 
                    sc.source.e++;
                    if (errorInEnd) {throw new Error("Error in End Test");}
                }
                async abort() { 
                    sc.source.a++;
                    if (errorInAbort) { throw new Error("Error in Abort Test"); } 
                }
                static ValidateConfig(config) { return []; }
                static async GetInstance(logger, config) { return new SpiedRecordSource(logger); }
            }

            class SpiedRecordTransformer extends AbstractRecordTransformer {
                constructor(logger) { super(logger); }
                async begin() { sc.transformer.b++;  }
                async transform(record) { 
                    sc.transformer.t++;
                    if (errorInXfrm) { throw new Error("Error in Xform Test"); }
                    const rtn = {data: record.data + 1};
                    d.xfrm.push(rtn);
                    return rtn;
                }
                async end() { sc.transformer.e++; }
                async abort() { sc.transformer.a++; }
                static ValidateConfig(config) { return []; }            
                static async GetInstance(logger, config) { return new SpiedRecordTransformer(logger); } 
            }

            class SpiedRecordLoader extends AbstractRecordLoader {
                constructor(logger) { super(logger); }
                async begin() { sc.loader.b++; }
                async loadRecord(record) { 
                    sc.loader.lr++; 
                    const rtn = {data: record.data + 1};
                    d.ldr.push(rtn);                    
                }
                async abort() { sc.loader.a++; }
                async end() { sc.loader.e++; }
                static ValidateConfig(config) { return []; }                
                static async GetInstance(logger, config) { return new SpiedRecordLoader(logger); }  
            }

            const config = {
                "source": { "module": SpiedRecordSource, "config": {} },
                "transformers": [ { "module": SpiedRecordTransformer, "config": {} } ],
                "loader": { "module": SpiedRecordLoader, "config": {} }
            };

            const processor = new PipelineProcessor(
                logger,
                config
            )

        return { sc, d, processor };
    }

    describe('run', async () => {

        it('Processes and Increments Count', async () => {

            const srcData = [{data: 1}];
            const { sc, d, processor } = SetupSpiedProcess(srcData);

            //This assertion count feels off, but is what works.
            //TODO: Figure out why a working version has a count of 3.
            expect.assertions(3);
            try {
                await processor.run();
            } catch (err) {
                //nothing
                expect(err).not.toBeTruthy();
            }
            

            expect(sc).toEqual({                
                    source: { b:1, gr:1, a:0, e:1 },
                    transformer: { b:1, t:1, a:0, e:1 },
                    loader: { b:1, lr:1, a:0, e:1 }                
            });

            expect(d).toEqual({                
                src: srcData,
                xfrm: [ {data:2}],
                ldr: [ {data:3}]  
            });

            expect(processor.recordsProcessed).toBe(1);
        })

        it('Error in processing', async () => {

            const srcData = [{data: 1}];
            const { sc, d, processor } = SetupSpiedProcess(srcData, true);

            expect.assertions(2);
            try {
                //Must be called here.
                await processor.run();
            } catch(err) {
                expect(err).toBeTruthy;
            }

            expect(sc).toEqual({                
                    source: { b:1, gr:1, a:1, e:0 },
                    transformer: { b:1, t:1, a:1, e:0 },
                    loader: { b:1, lr:0, a:1, e:0 }                
            });

            expect(d).toEqual({                
                src: srcData,
                xfrm: [],
                ldr: []  
            });

        })

        it('Error in Ending', async () => {

            const srcData = [{data: 1}];
            const { sc, d, processor } = SetupSpiedProcess(srcData, false, true);

            expect.assertions(2);
            try {
                //Must be called here.
                await processor.run();
            } catch(err) {
                expect(err).toBeTruthy;
            }

            expect(sc).toEqual({                
                    source: { b:1, gr:1, a:1, e:1 },
                    transformer: { b:1, t:1, a:1, e:1 },
                    loader: { b:1, lr:1, a:1, e:1 }                
            });

            expect(d).toEqual({                
                src: srcData,
                xfrm: [ {data:2}],
                ldr: [ {data:3}]  
            });

        })        

        it('Error in Abort', async () => {

            const srcData = [{data: 1}];
            const { sc, d, processor } = SetupSpiedProcess(srcData, false, false, true);

            expect.assertions(2);
            try {
                //Must be called here.
                await processor.run();
            } catch(err) {
                expect(err).toBeTruthy;
            }

            expect(sc).toEqual({                
                    source: { b:1, gr:1, a:0, e:1 },
                    transformer: { b:1, t:1, a:0, e:1 },
                    loader: { b:1, lr:1, a:0, e:1 }                
            });

            expect(d).toEqual({                
                src: srcData,
                xfrm: [ {data:2}],
                ldr: [ {data:3}]  
            });

        })
    })
    
})