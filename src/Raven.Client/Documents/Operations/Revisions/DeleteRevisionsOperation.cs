using System;
using System.Collections.Generic;
using System.Net.Http;
using Raven.Client.Documents.Conventions;
using Raven.Client.Http;
using Raven.Client.Json;
using Sparrow.Json;

namespace Raven.Client.Documents.Operations.Revisions;

public class DeleteRevisionsOperation : IMaintenanceOperation
{

    private readonly Parameters _parameters;

    public DeleteRevisionsOperation(Parameters parameters)
    {
        _parameters = parameters;
    }

    public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
    {
        return new DeleteRevisionsCommand(conventions, context, _parameters);
    }

    private class DeleteRevisionsCommand : RavenCommand
    {
        private readonly BlittableJsonReaderObject _parameters;

        public DeleteRevisionsCommand(DocumentConventions conventions, JsonOperationContext context, Parameters parameters)
        {
            if (conventions == null)
                throw new ArgumentNullException(nameof(conventions));
            if (context == null)
                throw new ArgumentNullException(nameof(context));
            if (parameters == null)
                throw new ArgumentNullException(nameof(parameters));

            _parameters = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(parameters, context);
        }
        public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
        {
            url = $"{node.Url}/databases/{node.Database}/admin/revisions";

            return new HttpRequestMessage
            {
                Method = HttpMethod.Delete,
                Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false))
            };
        }
    }
    
    public class Parameters
    {
        public List<string> DocumentIds { get; set; }
    }
}
