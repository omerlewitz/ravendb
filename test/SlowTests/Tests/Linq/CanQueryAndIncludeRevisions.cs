﻿using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Linq
{
    public class CanQueryAndIncludeRevisions : RavenTestBase
    {
        public CanQueryAndIncludeRevisions(ITestOutputHelper output) : base(output)
        {
        }
  
        [Fact]
        public async Task CanQueryAndIncludeRevisionsExtensionMethod()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/omer";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer",},
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ContractRevision, changeVector);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = session.Query<User>()
                        .Include(x => x.IncludeRevisions(x => x.ContractRevision));
                   
                    Assert.Equal("from 'Users' as x include revisions(x.ContractRevision)", query.ToString());

                    var queryResult = await query.ToListAsync();
                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
                
            }
        }

        [Fact]
        public async Task CanQueryAndIncludeSeveralRevisionsExtensionMethod()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/omer";
                const string id2 = "rhinos/omer";
                var cvList = new List<string>();
                
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer",}, id);
                    await session.StoreAsync(new User {Name = "Rhinos",RelatedDocument = "users/omer"}, id2);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                      
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);
                    
                    await session.SaveChangesAsync();

                    cvList.Add(changeVector);
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    
                    changeVector =  metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);
                    
                    await session.SaveChangesAsync(); 
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);
                    
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    // var t = session.Query<User>()
                    //     .Include(u => u.Name)
                    //     .Include(y => y.SecondRevision).ToString();
                    
                    var query = session.Query<User>()
                            .Include(u => u
                            .IncludeRevisions(x => x.RelatedDocument)
                            .IncludeRevisions(y => y.SecondRevision)
                            .IncludeRevisions(u => u.ThirdRevision));
                    
                    Assert.Equal("from 'Users' as u include revisions(u.Company,y.)", query.ToString());

                    //var queryResult = await query.ToListAsync();
                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);

                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
                
            }
        }

        [Fact]
        public async Task CanQueryAndIncludeRevisionsJint()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/omer";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer",},
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ContractRevision, changeVector);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.ContractRevision)
    return i;
}
from Users as u
where ID(u) = 'users/omer' 
select Foo(u)"
                        )
                        .ToListAsync();
                    
                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        
        [Fact]
        public async Task CanQueryAndIncludeRevisionsArrayJint()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/omer";
                var cvList = new List<string>();

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer",},
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                      
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);
                    
                    await session.SaveChangesAsync();

                    cvList.Add(changeVector);
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    
                    changeVector =  metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);
                    
                    await session.SaveChangesAsync(); 
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);
                    
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>(
                            @"
declare function Foo(i) {
    includes.revisions(i.FirstRevision, i.SecondRevision, i.ThirdRevision)
    return i;
}
from Users as u
where ID(u) = 'users/omer' 
select Foo(u)"
                        )
                        .ToListAsync();
                    
                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);
                
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task CanQueryAndIncludeRevisionsTest()
        {
            using (var store = GetDocumentStore())
            {
                const string id = "users/omer";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer",},
                        id);

                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);

                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);

                    session.Advanced.Patch<User, string>(id, x => x.ContractRevision, changeVector);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users include revisions($p0)")
                        .AddParameter("p0", "ContractRevision")
                        .ToListAsync();

                    var revision = await session.Advanced.Revisions.GetAsync<User>(changeVector);

                    Assert.NotNull(revision);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public void  CanQueryAndIncludeRevisionsArrayInsideProperty()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();
                
                const string id = "users/omer";
                
                 RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                
                using (var session = store.OpenSession())
                {
                     session.Store(new User
                        {
                            Name = "Omer",
                        },
                        id);
                 
                     session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas =  session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);
                      
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);
                    
                    session.SaveChanges(); 
                    
                    cvList.Add(changeVector);
                    
                    metadatas =  session.Advanced.Revisions.GetMetadataFor(id);
                    
                    changeVector =  metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);
                    
                    session.SaveChanges(); 
                    
                    metadatas =  session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);

                    session.Advanced.Patch<User, List<string>>(id, x => x.ChangeVectors, cvList);

                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                   
                    var query =  session.Advanced
                        .RawQuery<User>("from Users include revisions(ChangeVectors)")
                        .ToList();
                     
                    var revision1 =  session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 =  session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 =  session.Advanced.Revisions.Get<User>(cvList[2]);
                
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
                
            }
        }
        
        [Fact]
        public void  CanQueryAndIncludeRevisionsArrayWithCache()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();
                
                const string id = "users/omer";
                
                 RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                
                using (var session = store.OpenSession())
                {
                     session.Store(new User
                        {
                            Name = "Omer",
                        },
                        id);
                    
                     session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas =  session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);
                      
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);
                    
                     session.SaveChanges(); 
                    
                    cvList.Add(changeVector);
                    
                    metadatas =  session.Advanced.Revisions.GetMetadataFor(id);
                    
                    changeVector =  metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);
                    
                    session.SaveChanges(); 
                    
                    metadatas =  session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);
                    
                    session.SaveChanges();
                    
                }
                using (var session = store.OpenSession())
                {
                    var query =  session.Advanced
                        .RawQuery<User>("from Users as u include revisions($p0, $p1, $p2)")
                        .AddParameter("p0","u.FirstRevision")
                        .AddParameter("p1","u.SecondRevision")
                        .AddParameter("p2","u.ThirdRevision")
                        .ToList();
                    
                    var revision1 =  session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 =  session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 =  session.Advanced.Revisions.Get<User>(cvList[2]);
                
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        
        [Fact]
        public async Task CanQueryAndIncludeRevisionsArrayWithCacheAsync()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();
                
                const string id = "users/omer";
                
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                        {
                            Name = "Omer",
                        },
                        id);
                    
                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                      
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);
                    
                    await session.SaveChangesAsync(); 
                    
                    cvList.Add(changeVector);
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    
                    changeVector =  metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);
                    
                    await session.SaveChangesAsync(); 
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);
                    
                    await session.SaveChangesAsync();
                    
                }
                
                using (var session = store.OpenAsyncSession())
                {
                     var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users as u include revisions(u.FirstRevision, u.SecondRevision,u.ThirdRevision)")
                        .ToListAsync();
                
                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);
                
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users as u include revisions($p0, $p1, $p2)")
                        .AddParameter("p0","u.FirstRevision")
                        .AddParameter("p1","u.SecondRevision")
                        .AddParameter("p2","u.ThirdRevision")
                        .ToListAsync();
                    
                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);
                
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }

        [Fact]
        public async Task CanQueryAndIncludeRevisionsAliasSyntaxError()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/omer";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                        {
                            Name = "Omer",
                        },
                        id);
                    
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var error =  Assert.ThrowsAny<RavenException>(  () => session.Advanced
                        .RawQuery<User>("from Users as u include revisions(u.FirstRevision, x.SecondRevision)")
                        .ToList());
                        
                    Assert.Contains("System.InvalidOperationException: Cannot include revisions for related Expression '<Field>: x.SecondRevision', Parent alias is different than include alias 'u' compare to 'x';"
                        , error.Message);
                }
                using (var session = store.OpenSession())
                {
                    
                    var error =  Assert.ThrowsAny<RavenException>( () =>  session.Advanced
                        .RawQuery<User>("from Users as u include revisions($p0, $p1, $p2)")
                        .AddParameter("p0", "u.FirstRevision")
                        .AddParameter("p1", "u.SecondRevision")
                        .AddParameter("p2", "x.ThirdRevision")
                        .ToList());
                    
                    Assert.Contains(@"System.InvalidOperationException: Cannot include revisions for related Expression '(x.ThirdRevision, String)', Parent alias is different than include alias 'u' compare to 'x';. "
                        , error.Message);
                    
                }
            }
        }

        [Fact]
        public async Task CanQueryAndIncludeRevisionsAliasSyntaxErrorAsync()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();

                const string id = "users/omer";

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                        {
                            Name = "Omer",
                        },
                        id);
                    
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var error = await Assert.ThrowsAnyAsync<RavenException>( async () =>  await session.Advanced
                        .AsyncRawQuery<User>("from Users as u include revisions(u.FirstRevision, x.SecondRevision)")
                        .ToListAsync());
                        
                    Assert.Contains("System.InvalidOperationException: Cannot include revisions for related Expression '<Field>: x.SecondRevision', Parent alias is different than include alias 'u' compare to 'x';"
                                    , error.Message);
                }
                using (var session = store.OpenAsyncSession())
                {
                    
                    var error = await Assert.ThrowsAnyAsync<RavenException>(async () => await session.Advanced
                        .AsyncRawQuery<User>("from Users as u include revisions($p0, $p1, $p2)")
                        .AddParameter("p0", "u.FirstRevision")
                        .AddParameter("p1", "u.SecondRevision")
                        .AddParameter("p2", "x.ThirdRevision")
                        .ToListAsync());
                    
                    Assert.Contains(@"System.InvalidOperationException: Cannot include revisions for related Expression '(x.ThirdRevision, String)', Parent alias is different than include alias 'u' compare to 'x';. "
                        , error.Message);
                    
                }
            }
        }
        
        [Fact]
        public void  CanQueryAndIncludeRevisionsWithCache()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();
                
                const string id = "users/omer";
                
                 RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                
                using (var session = store.OpenSession())
                {
                     session.Store(new User
                        {
                            Name = "Omer",
                        },
                        id);
                    
                     session.SaveChanges();
                }

                string changeVector;
                using (var session = store.OpenSession())
                {
                    var metadatas =  session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(1, metadatas.Count);
                      
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);
                    
                     session.SaveChanges(); 
                    
                    cvList.Add(changeVector);
                    
                    metadatas =  session.Advanced.Revisions.GetMetadataFor(id);
                    
                    changeVector =  metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);
                    
                    session.SaveChanges(); 
                    
                    metadatas =  session.Advanced.Revisions.GetMetadataFor(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);
                    
                    session.SaveChanges();
                    
                }
                
                using (var session = store.OpenSession())
                {
                     var query =  session.Advanced
                        .RawQuery<User>("from Users as u include revisions(u.FirstRevision, u.SecondRevision,u.ThirdRevision)")
                        .ToList();
                     
                     var revision1 =  session.Advanced.Revisions.Get<User>(cvList[0]);
                     var revision2 =  session.Advanced.Revisions.Get<User>(cvList[1]);
                     var revision3 =  session.Advanced.Revisions.Get<User>(cvList[2]);
                
                     Assert.NotNull(revision1);
                     Assert.NotNull(revision2);
                     Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
                
                using (var session = store.OpenSession())
                {
                    var query =  session.Advanced
                        .RawQuery<User>("from Users as u include revisions($p0),revisions($p1),revisions($p2)")
                        .AddParameter("p0","u.FirstRevision")
                        .AddParameter("p1","u.SecondRevision")
                        .AddParameter("p2","u.ThirdRevision")
                        .ToList();
                    
                    var revision1 =  session.Advanced.Revisions.Get<User>(cvList[0]);
                    var revision2 =  session.Advanced.Revisions.Get<User>(cvList[1]);
                    var revision3 =  session.Advanced.Revisions.Get<User>(cvList[2]);
                
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        
        [Fact]
        public async Task CanQueryAndIncludeRevisionsWithCacheAsync()
        {
            using (var store = GetDocumentStore())
            {
                var cvList = new List<string>();
                
                const string id = "users/omer";
                
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                        {
                            Name = "Omer",
                        },
                        id);
                    
                    await session.SaveChangesAsync();
                }

                string changeVector;
                using (var session = store.OpenAsyncSession())
                {
                    var metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(1, metadatas.Count);
                      
                    changeVector = metadatas.First().GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.FirstRevision, changeVector);
                    
                    await session.SaveChangesAsync(); 
                    
                    cvList.Add(changeVector);
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    
                    changeVector =  metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.SecondRevision, changeVector);
                    
                    await session.SaveChangesAsync(); 
                    
                    metadatas = await session.Advanced.Revisions.GetMetadataForAsync(id);

                    changeVector = metadatas[0].GetString(Constants.Documents.Metadata.ChangeVector);
                    
                    cvList.Add(changeVector);
                    
                    session.Advanced.Patch<User, string>(id, x => x.ThirdRevision, changeVector);
                    
                    await session.SaveChangesAsync();
                    
                }
                using (var session = store.OpenAsyncSession())
                {
                    var query = await session.Advanced
                        .AsyncRawQuery<User>("from Users as u include revisions($p0),revisions($p1),revisions($p2)")
                        .AddParameter("p0","u.FirstRevision")
                        .AddParameter("p1","u.SecondRevision")
                        .AddParameter("p2","u.ThirdRevision")
                        .ToListAsync();
                    
                    var revision1 = await session.Advanced.Revisions.GetAsync<User>(cvList[0]);
                    var revision2 = await session.Advanced.Revisions.GetAsync<User>(cvList[1]);
                    var revision3 = await session.Advanced.Revisions.GetAsync<User>(cvList[2]);
                
                    Assert.NotNull(revision1);
                    Assert.NotNull(revision2);
                    Assert.NotNull(revision3);
                    
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
  
        private class User
        {
            public string Name { get; set; }
            public string ContractRevision { get; set; }
            public string RelatedDocument { get; set; }
            public string FirstRevision { get; set; }
            public string SecondRevision { get; set; }
            public string ThirdRevision { get; set; }
            public List<string> ChangeVectors { get; set; } 
        }
    }
}
