using System;
using System.Collections.Generic;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Includes
{
    public class IncludeRevisionsCommand
    {
        private readonly DocumentDatabase _database;
        private readonly DocumentsOperationContext _context;
        private readonly HashSet<string> _pathsForRevisionsInDocuments;
        private readonly DateTime? _dateTime;
        private readonly Dictionary<string, (long start, long take)> _pathsForRevisionsRelatedDocuments;

        public Dictionary<string, Document> Results { get; private set; }

        private IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context)
        {
            _database = database;
            _context  = context;
        }
        
        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, HashSet<string> pathsForRevisionsInDocuments) : this(database, context)
        {
            _pathsForRevisionsInDocuments = pathsForRevisionsInDocuments ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }
        
        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, Dictionary<string, (long start, long take)> pathsForRevisionsRelatedDocuments) : this(database, context)
        {
            _pathsForRevisionsRelatedDocuments = pathsForRevisionsRelatedDocuments ?? new Dictionary<string,  (long start, long take)>(StringComparer.OrdinalIgnoreCase);
        }
        
        public IncludeRevisionsCommand(
            DocumentDatabase database, DocumentsOperationContext context, 
            HashSet<string> pathsForRevisionsInDocuments, Dictionary<string, (long start, long take)> pathsForRevisionsRelatedDocuments,
            DateTime? revisionIncludesRevisionWithDateTime) : this(database, context)
        {
            _pathsForRevisionsInDocuments = pathsForRevisionsInDocuments ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            _pathsForRevisionsRelatedDocuments = pathsForRevisionsRelatedDocuments ?? new Dictionary<string,  (long start, long take)>(StringComparer.OrdinalIgnoreCase);
            _dateTime ??= revisionIncludesRevisionWithDateTime;
        }

        public IncludeRevisionsCommand(DocumentDatabase database, DocumentsOperationContext context, DateTime? dateTime) : this(database, context)
        {
            _dateTime = dateTime;
        }
        
        public void Fill(Document document)
        {
            if (document == null)
                return;

            if (_dateTime != null)
            {
                var doc  = _database.DocumentsStorage.RevisionsStorage.GetRevisionBefore(context:_context,id: document.Id, max: _dateTime.Value);
                if(doc is null) return;
                Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                Results[doc.ChangeVector] = doc;
            }

            if (_pathsForRevisionsRelatedDocuments != null)
            {
                foreach ((string key, (long start, long take)) in _pathsForRevisionsRelatedDocuments)
                {
                    var tryGetId = document.Data.TryGet(key, out string id);
                    if(tryGetId == false)
                        continue;
                
                    var revisions  = _database.DocumentsStorage.RevisionsStorage.GetRevisions(context: _context, id:id, start:start, take:take);
                    Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                    foreach (var doc in revisions.Revisions)
                    {
                        Results[doc.ChangeVector] = doc;
                    }
                }

            }

            if (_pathsForRevisionsInDocuments != null)
            {
                foreach (var fieldName in _pathsForRevisionsInDocuments)
                {
                    if (document.Data.TryGet(fieldName, out object singleOrMultipleCv) == false  )
                    {
                        throw new InvalidOperationException($"Cannot include revisions for related document '{document.Id}', " +
                                                            $"document {document.Id} doesn't have a field named '{fieldName}'. ");
                    }

                    switch (singleOrMultipleCv)
                    {
                        case BlittableJsonReaderArray blittableJsonReaderArray:
                        {
                            foreach (object cvObj in blittableJsonReaderArray)
                            {
                                var changeVector = Convert.ToString(cvObj);
                                var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:changeVector);
                                Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                                Results[changeVector] = getRevisionsByCv;
                            }
                            break;
                        }
                        case LazyStringValue lazyStringValue:
                        {
                            var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:lazyStringValue);
                            Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                            Results[lazyStringValue] = getRevisionsByCv;
                            break;
                        }
                    }
                }
            }
        
        }

        public void AddRange(IEnumerable<string> revisionsCvs)
        {
            if (revisionsCvs is null)
                return;
            
            foreach (string revisionsCv in revisionsCvs)
            {
                var getRevisionsByCv  = _database.DocumentsStorage.RevisionsStorage.GetRevision(context: _context, changeVector:revisionsCv);
                Results ??= new Dictionary<string, Document>(StringComparer.OrdinalIgnoreCase);
                Results[revisionsCv] = getRevisionsByCv;
            }  
        }
    }
}
