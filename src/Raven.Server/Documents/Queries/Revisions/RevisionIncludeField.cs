using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Revisions
{
    public class RevisionIncludeField
    {
        public HashSet<string> RevisionsChangeVectorSet;
        public DateTime? RevisionWithDateTime;
        public Dictionary<string, (long start, long take)> RevisionsWithPaging;

        public void AddRevision(string field)
        {
            RevisionsChangeVectorSet ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            RevisionsChangeVectorSet.Add(field);
        }
        
        public void AddRevision(string path, long start, long take)
        {
            RevisionsWithPaging ??= new Dictionary<string, (long start, long take)>(StringComparer.OrdinalIgnoreCase);
            RevisionsWithPaging.Add(path, (start,take));
        }
        
        public void AddRevision(DateTime dateTime)
        {
            RevisionWithDateTime = dateTime;
        }
    }
}
