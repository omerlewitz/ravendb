using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sparrow.Extensions;

namespace Raven.Client.Documents.Session.Tokens
{
    public class RevisionIncludesToken : QueryToken
    {
        private readonly string   _id;
        private string _sourcePath;
        private bool _isFirst = true;
        private readonly long _start;
        private readonly long _take;
        private readonly DateTime? _dateTime = null;

        private RevisionIncludesToken(string sourcePath, string id, (long start, long take) pagingTuple)
        {
            _sourcePath = sourcePath;
            _id = id;
            (_start, _take) = pagingTuple;

        }
        
        private RevisionIncludesToken(string sourcePath, DateTime dateTime)
        {
            _sourcePath = sourcePath;
            _dateTime = dateTime;
        }
        
        internal static RevisionIncludesToken Create(string sourcePath, DateTime dateTime)
        {
            return new RevisionIncludesToken(sourcePath, dateTime);
        }
        
        internal static RevisionIncludesToken Create(string sourcePath, string id, (long Start, long Take) pagingTuple)
        {
            return new RevisionIncludesToken(sourcePath, id, pagingTuple);
        }
        
        public void AddAliasToPath(string alias)
        {
            _sourcePath = _sourcePath == string.Empty
                ? alias
                : $"{alias}.{_id}";
        }
   
        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("revisions(");

            if (_dateTime is not null)
            {
                writer.Append('\'');
                writer.Append(_dateTime.Value.GetDefaultRavenFormat());
                writer.Append('\'');
                writer.Append(')');
            }
            
            if (string.IsNullOrEmpty(_id) == false)
            {
                writer.Append('\'');
                writer.Append(_id);
                writer.Append('\'');
                writer.Append(',');
                writer.Append(_start);
                writer.Append(',');
                writer.Append(_take);
                writer.Append(')');
            }
            
            


        }
      
    }
}
