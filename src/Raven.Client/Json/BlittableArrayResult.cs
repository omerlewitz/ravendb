using System.Collections.Generic;
using Sparrow.Json;

namespace Raven.Client.Json
{
    public class BlittableArrayResult
    {
        public BlittableJsonReaderArray Results { get; set; }
    }

    public class BatchCommandResult
    {
        public BlittableJsonReaderArray Results { get; set; }
        public long? TransactionIndex { get; set; }
    }
    public class BatchCommandResult2
    {
        public List<string> Ids { get; set; }
    }
}
