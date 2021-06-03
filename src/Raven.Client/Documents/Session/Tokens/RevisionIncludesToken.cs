using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class RevisionIncludesToken : QueryToken
    {
        private readonly  string _path;
        private string   _sourcePath;
        private bool     _isFirst = true;

        public RevisionIncludesToken(string sourcePath, string path)
        {
            _sourcePath = sourcePath;
            _path = path;
                    
        }
        
        internal static RevisionIncludesToken Create(string sourcePath, string path)
        {
            return new RevisionIncludesToken(sourcePath, path);
        }

        public void AddAliasToPath(string alias)
        {
            _sourcePath = _sourcePath == string.Empty
                    ? alias
                    : $"{alias}.{_sourcePath}";
            
        }
   
        public override void WriteTo(StringBuilder writer)
        {
               writer.Append("revisions(");

               foreach (var field in _path)
               {
                   if(string.IsNullOrEmpty(_sourcePath) == false)
                       writer.Append(_sourcePath);
                        
                   if(_isFirst is false)
                       writer.Append(',');
                    
                   writer.Append('\'');
                   writer.Append(field);
                   writer.Append('\'');
                   _isFirst = !_isFirst;
               }
                
               writer.Append(')');
        }

    }
}
