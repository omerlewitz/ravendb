using System;
using System.Text;

namespace Raven.Client.Documents.Session.Tokens
{
    public class RevisionIncludesToken : QueryToken
    {
        private  string _path;
        
        public RevisionIncludesToken(string path)
        {
            _path = path ?? throw new ArgumentNullException(nameof(path));
        }
        
        internal static RevisionIncludesToken Create(string path)
        {
            return new RevisionIncludesToken(path);
        }
        
        public void AddAliasToPath(string alias)
        {
            _path = _path == string.Empty ? alias : $"{alias}.{_path}";
        }
        
        public override void WriteTo(StringBuilder writer)
        {
            writer.Append("revisions(");

            if (_path != string.Empty)
            {
                writer.Append('\'');
                writer.Append(_path);

                // if (_all == false)
                //     writer.Append(", ");
            }

            // if (_all == false)
            // {
            //     writer.Append("'");
            //     writer.Append(_counterName);
            //     writer.Append("'");
            // }

            writer.Append('\'');
            writer.Append(")");
        }
    }
}
