using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using Raven.Client.Documents.Conventions;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.Json.Serialization;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions;

public class GetRevisionsBinOperation : IMaintenanceOperation<BatchCommandResult2>
{
    private readonly long _etag;
    private readonly int? _pageSize;

    public GetRevisionsBinOperation(long etag, int? pageSize)
    {
        _etag = etag;
        _pageSize = pageSize;
    }

    public RavenCommand<BatchCommandResult2> GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new GetRevisionsBinEntryCommand(_etag, _pageSize);
    }

    public class GetRevisionsBinEntryCommand : RavenCommand<BatchCommandResult2>
    {
        private readonly long _etag;
        private readonly int? _pageSize;

        public GetRevisionsBinEntryCommand(long etag, int? pageSize)
        {
            _etag = etag;
            _pageSize = pageSize;
        }

        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            var request = new HttpRequestMessage {Method = HttpMethod.Get};

            var pathBuilder = new StringBuilder(node.Url);
            pathBuilder.Append("/databases/")
                .Append(node.Database)
                .Append("/revisions/bin?etag=")
                .Append(_etag);

            if (_pageSize.HasValue)
                pathBuilder.Append("&pageSize=").Append(_pageSize);

            url = pathBuilder.ToString();
            return request;
        }

        public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
        {
            if (response == null)
                throw new InvalidOperationException();
            if (fromCache)
            {
                // we have to clone the response here because  otherwise the cached item might be freed while
                // we are still looking at this result, so we clone it to the side
                response = response.Clone(context);
            }

            var result = new BatchCommandResult2();
            result.Ids = new List<string>();
            if (response.TryGet("Results", out BlittableJsonReaderArray array))
            {
                foreach (BlittableJsonReaderObject obj in array)
                {
                    if (obj.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                        metadata.TryGet(Constants.Documents.Metadata.Id, out string id))
                    {
                        result.Ids.Add(id);
                    }
                }
            }

            Result = result;
        }

        public override bool IsReadRequest => true;
    }
}
