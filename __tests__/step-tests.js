/** This file is for testing the checks for implementing the abstract steps */
const winston               = require('winston');
const WinstonNullTransport  = require('winston-null-transport');

const AbstractPipelineStep = require('../lib/abstract-pipeline-step');
const {
    AbstractRecordSource, AbstractRecordTransformer, AbstractRecordLoader
} = require('../index');

const logger = winston.createLogger({
    level: 'debug',
    format: winston.format.simple(),
    transports: [
        new WinstonNullTransport()
    ]
})


describe('AbstractPipelineStep', () => {

    it('throws on instantiation', () => {
        expect((() => {
            new AbstractPipelineStep(logger);
        })).toThrow("Cannot construct AbstractPipelineStep")
    })

    it('throws on missing ValidateConfig', () => {
        expect((() => {
            AbstractPipelineStep.ValidateConfig();
        })).toThrow("Cannot call abstract static method ValidateConfig from derrived class")
    })

    it('throws on missing GetInstance', async () => {
        expect.assertions(1);

        try {
            await AbstractPipelineStep.GetInstance(logger, {});
        } catch (err) {
            expect(err.message).toEqual("Cannot call abstract static method GetInstance from derrived class");
        }
    })


    it('must implement begin', () => {
        class Test extends AbstractPipelineStep {
            constructor(logger) { super(logger); }
            async end() {}
            async abort() {}
        }

        expect(() => {
            new Test();
        }).toThrow("Must implement abstract method begin");
    })

    it('must implement abort', () => {
        class Test extends AbstractPipelineStep {
            constructor(logger) { super(logger); }
            async begin() {}
            async end() {}
        }
        expect(() => {
            new Test();
        }).toThrow("Must implement abstract method abort");

    })

    it('must implement end', () => {
        class Test extends AbstractPipelineStep {
            constructor(logger) { super(logger); }
            async begin() {}
            async abort() {}
        }
        expect(() => {
            new Test();
        }).toThrow("Must implement abstract method end");

    })

})

describe('AbstractRecordSource', () => {
    it('throws on instantiation', () => {
        expect((() => {
            new AbstractRecordSource(logger);
        })).toThrow("Must implement abstract method begin") //Because of how super works
    })

    it('must implement getRecords', () => {
        class Test extends AbstractRecordSource {
            constructor(logger) { super(logger); }
            async begin() {}
            async end() {}
            async abort() {}
        }

        expect(() => {
            new Test();
        }).toThrow("Must implement abstract method getRecords");
    })
})

describe('AbstractRecordTransformer', () => {
    it('throws on instantiation', () => {
        expect((() => {
            new AbstractRecordTransformer(logger);
        })).toThrow("Must implement abstract method begin") //Because of how super works
    })

    it('must implement transform', () => {
        class Test extends AbstractRecordTransformer {
            constructor(logger) { super(logger); }
            async begin() {}
            async end() {}
            async abort() {}
        }

        expect(() => {
            new Test();
        }).toThrow("Must implement abstract method transform");
    })
})

describe('AbstractRecordLoader', () => {
    it('throws on instantiation', () => {
        expect((() => {
            new AbstractRecordLoader(logger);
        })).toThrow("Must implement abstract method begin") //Because of how super works
    })

    it('must implement loadRecord', () => {
        class Test extends AbstractRecordLoader {
            constructor(logger) { super(logger); }
            async begin() {}
            async end() {}
            async abort() {}
        }

        expect(() => {
            new Test();
        }).toThrow("Must implement abstract method loadRecord");
    })
})