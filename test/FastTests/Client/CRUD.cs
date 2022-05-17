using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests.Server.Documents.Revisions;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class CRUD : RavenTestBase
    {
        public CRUD(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CRUD_Operations()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                var id2 = "users/2";
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration> {["Users"] = new RevisionsCollectionConfiguration {Disabled = false,}}
                };
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration);
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Omer"}, id);
                    session.Store(new User {Name = "Rhinos"}, id2);
                    session.SaveChanges();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Store(new User {Name = $"Omer{i}"}, id);
                        session.Store(new User {Name = $"Rhinos{i}"}, id2);
                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Delete(id);
                    session.Delete(id2);
                    session.SaveChanges();
                }

                var operation = new GetRevisionsBinOperation(long.MaxValue, int.MaxValue);
                var results= await store.Maintenance.SendAsync(operation);
                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(new DeleteRevisionsOperation.Parameters {DocumentIds =  results.Ids}));
                
            }
        }
    }
}
