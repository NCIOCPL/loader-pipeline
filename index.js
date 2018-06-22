const PipelineProcessor         = require('./lib/pipeline-processor');
const AbstractRecordLoader      = require('./lib/abstract-record-loader');
const AbstractRecordSource      = require('./lib/abstract-record-source');
const AbstractRecordTransformer = require('./lib/abstract-record-transformer');

module.exports = {
    PipelineProcessor,
    AbstractRecordLoader,
    AbstractRecordSource,
    AbstractRecordTransformer
}
