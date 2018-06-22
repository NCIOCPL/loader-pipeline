const winston               = require('winston');
const WinstonNullTransport  = require('winston-null-transport');
const path                  = require('path');

const { PipelineProcessor }   = require('../index');

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


    describe('Constructor Error Handling', async () => {

        const goodStepInfo = Object.freeze({
            type: "string", 
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
            type: {}, 
            config: "string" 
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



/*
 * This file contains the tests for a basic flow through the processor.
 * Source generates 1 object -> Transform Returns it -> Loader does nothing.
 */

    describe('loadPipeline', async () => {        

        const goodConfig = {
            "source": {
                "type": path.join(__dirname, "test-steps", "sources", "test-record-source"),
                "config": {}
            },
            "transformers": [
                {
                    "type": path.join(__dirname, "test-steps", "transformers", "test-record-transformer"),
                    "config": {}
                }
            ],
            "loader": {
                "type": path.join(__dirname, "test-steps", "loaders", "test-record-loader"),
                "config": {}
            }
        };

        const processor = new PipelineProcessor(
            logger,
            goodConfig    
        )
        
        it('loads', async () => {
            await processor.loadPipeline();
    
            expect(processor.sourceStep).toBeInstanceOf(TestRecordSource);
            expect(processor.transformerSteps).toHaveLength(1);
            expect(processor.transformerSteps[0]).toBeInstanceOf(TestRecordTransformer);
            expect(processor.loaderStep).toBeInstanceOf(TestRecordLoader);
        });
    
        it('encounters an error', async () => {
            const processor = new PipelineProcessor(
                logger,
                { ...goodConfig, source: { type: "badpath", config: {} } }
            );
            
            expect.assertions(1);
            try {
                await processor.loadPipeline();
            } catch (err) {
                expect(err).toMatchObject({
                    code: "MODULE_NOT_FOUND"
                });
            }
        });
    })

    describe('loadPipelineStep', async () => {
        const PATH_TO_MODULE = path.join(__dirname, 'test-steps', 'processor-error-test-step')

        const goodStepInfo = Object.freeze({
            type: "string", 
            config: {}
        });
        
        const goodConfig = Object.freeze({
            source: goodStepInfo,
            transformers: [
                goodStepInfo
            ],
            loader: goodStepInfo
        });
    
        const processor = new PipelineProcessor(logger, goodConfig);
    
        ////////////////////////
        /// Everything worked ok
        ////////////////////////
        it('loads the step', async() => {
            let actual = await processor.loadPipelineStep(PATH_TO_MODULE, {});
            expect(actual).toBeTruthy();
        });
    
        //////////////////////
        /// Error Cases
        //////////////////////
        describe('throws on', async() => {
            it('inability to load the module', async () => {
    
                expect.assertions(1);
                try {
                     await processor.loadPipelineStep('/path/does/not/exist', {});
                } catch (err) {
                    expect(err).toMatchObject({
                        code: "MODULE_NOT_FOUND"
                    });
                }
    
            });
        
            it('invalid step configuration by exception', async () => {
                expect.assertions(1);
                try {
                    await processor.loadPipelineStep(
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

})