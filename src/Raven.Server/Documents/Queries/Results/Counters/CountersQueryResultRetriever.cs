﻿using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries.Results.Counters
{
    public class CountersQueryResultRetriever : TimeSeriesQueryResultRetriever
    {
        public CountersQueryResultRetriever(DocumentDatabase database, IndexQueryServerSide query, QueryTimingsScope queryTimings, DocumentsStorage documentsStorage, JsonOperationContext context, FieldsToFetch fieldsToFetch, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand)
            : base(database, query, queryTimings, documentsStorage, context, fieldsToFetch, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand)
        {
        }
    }
}
