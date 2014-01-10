﻿// CqlSharp.Linq - CqlSharp.Linq.Test
// Copyright (c) 2014 Joost Reuzel
//   
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
// http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace CqlSharp.Linq.Test
{
    [TestClass]
    public class LinqTest
    {
        private void ExecuteQuery(QueryFunc query, string cql)
        {
            using (var queryWriter = new StringWriter())
            using (var context = new MyContext {SkipExecute = true, Log = queryWriter})
            {
                var result = query(context);
                Assert.AreEqual(cql, queryWriter.ToString().TrimEnd());
            }
        }

        [TestMethod]
        public void WhereThenSelect()
        {
            var filter = "hallo";

            QueryFunc query =
                (context) => context.Values.Where(p => p.Value == filter + " daar").Select(r => r.Id).ToList();

            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' WHERE 'value'='hallo daar';");
        }

        [TestMethod]
        public void SelectThenWhere()
        {
            QueryFunc query = context => context.Values.Select(r => r.Id).Where(id => id == 4).ToList();

            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' WHERE 'id'=4;");
        }

        [TestMethod]
        public void NoWhereOrSelect()
        {
            QueryFunc query = context => context.Values.ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void SelectAll()
        {
            QueryFunc query = context => context.Values.Select(row => row).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void SelectIntoNewObject()
        {
            QueryFunc query = context => context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue';");
        }

        [TestMethod]
        public void WhereIdInArray()
        {
            QueryFunc query = context => context.Values.Where(r => new[] {1, 2, 3, 4}.Contains(r.Id)).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInList()
        {
            QueryFunc query = context => context.Values.Where(r => new List<int> {1, 2, 3, 4}.Contains(r.Id)).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        public void WhereIdInSet()
        {
            QueryFunc query =
                context => context.Values.Where(r => new HashSet<int> {1, 2, 3, 4}.Contains(r.Id)).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (1,2,3,4);");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException),
            "Type System.Collections.Generic.KeyValuePair`2[System.Int32,System.String] can not be converted to a valid CQL value"
            )]
        public void WhereKvpInDictionary()
        {
            QueryFunc query =
                context =>
                context.Values.Where(
                    r =>
                    new Dictionary<int, string> {{1, "a"}, {2, "b"}, {3, "c"}}.Contains(
                        new KeyValuePair<int, string>(r.Id, "a"))).ToList();
            ExecuteQuery(query, "No valid query");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException), "Type System.Char can't be converted to a CQL value")]
        public void WhereIdInNotSupportedListType()
        {
            QueryFunc query =
                context => context.Values.Where(r => new List<char> {'a', 'b', 'c'}.Contains((char) r.Id)).ToList();
            ExecuteQuery(query, "No valid query");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhere()
        {
            QueryFunc query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).Where(at => at.Id2 == 4).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=4;");
        }

        [TestMethod]
        public void SelectThenSelect()
        {
            QueryFunc query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id + 2, Value2 = r.Value}).Select(r2 => new {Id3 = r2.Id2}).
                    ToList();
            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue';");
        }

        [TestMethod]
        public void OnlyWhere()
        {
            QueryFunc query = context => context.Values.Where(r => r.Id == 2).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException), "CQL does not support the Add operator")]
        public void UnParsableWhereQuery()
        {
            QueryFunc query = context => context.Values.Where(r => r.Id + 2 == 4).ToList();
            ExecuteQuery(query, "no valid query");
        }

        [TestMethod]
        //[ExpectedException(typeof(CqlLinqException), "CQL does not support the Add operator")]
        public void WhereFromLinqToObjects()
        {
            var range = Enumerable.Range(1, 5);
            var selection = from r in range where r > 3 select r;

            QueryFunc query = context => context.Values.Where(r => selection.AsQueryable().Contains(r.Id)).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id' IN (4,5);");
        }

        [TestMethod]
        public void OnlyFirst()
        {
            QueryFunc query = context => context.Values.First();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void FirstWithPredicate()
        {
            QueryFunc query = context => context.Values.First(v => v.Id == 2);
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2 LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenFirst()
        {
            QueryFunc query = context => context.Values.Select(v => new {Id2 = v.Id}).First();
            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenWhereThenFirst()
        {
            QueryFunc query = context => context.Values.Select(v => new {Id2 = v.Id}).Where(v2 => v2.Id2 == 2).First();
            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' WHERE 'id'=2 LIMIT 1;");
        }

        [TestMethod]
        public void SelectThenFirstWithPredicate()
        {
            QueryFunc query = context => context.Values.Select(v => new {Id2 = v.Id}).First(v2 => v2.Id2 == 2);
            ExecuteQuery(query, "SELECT 'id' FROM 'myvalue' WHERE 'id'=2 LIMIT 1;");
        }

        [TestMethod]
        public void OnlyFirstOrDefault()
        {
            QueryFunc query = context => context.Values.FirstOrDefault();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void FirstOrDefaultWithPredicate()
        {
            QueryFunc query = context => context.Values.FirstOrDefault(v => v.Id == 2);
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2 LIMIT 1;");
        }

        [TestMethod]
        public void CountWithPredicate()
        {
            QueryFunc query = context => context.Values.Count(v => v.Id == 2);
            ExecuteQuery(query, "SELECT COUNT(*) FROM 'myvalue' WHERE 'id'=2;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeBeforeWhere()
        {
            //Wrong: logically first three items of values  table are taken, then where is performed on those three values, but Cql does not support sub-queries so this will not provide expected results
            QueryFunc query = context => context.Values.Take(3).Where(v => v.Id == 2).ToList();
            ExecuteQuery(query, "invalid query");
        }

        [TestMethod]
        public void WhereThenTake()
        {
            QueryFunc query = context => context.Values.Where(v => v.Id == 2).Take(3).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=2 LIMIT 3;");
        }

        [TestMethod]
        public void LargeTakeThenSmallTake()
        {
            QueryFunc query = context => context.Values.Take(3).Take(1).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void SmallTakeThenLargeTake()
        {
            QueryFunc query = context => context.Values.Take(1).Take(3).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' LIMIT 1;");
        }

        [TestMethod]
        public void TakeThenCount()
        {
            QueryFunc query = context => context.Values.Take(100).Count();
            ExecuteQuery(query, "SELECT COUNT(*) FROM 'myvalue' LIMIT 100;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeThenCountWithCondition()
        {
            QueryFunc query = context => context.Values.Take(100).Count(v => v.Id > 100);
            ExecuteQuery(query, "invalid query");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenWhereThenTake()
        {
            QueryFunc query =
                context =>
                context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).Where(at => at.Id2 == 4).Take(3).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' WHERE 'id'=4 LIMIT 3;");
        }

        [TestMethod]
        public void OrderBy()
        {
            QueryFunc query = context => context.Values.OrderBy(v => v.Id).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' ORDER BY 'id' ASC;");
        }

        [TestMethod]
        public void SelectIntoNewObjectThenOrderBy()
        {
            QueryFunc query =
                context => context.Values.Select(r => new {Id2 = r.Id, Value2 = r.Value}).OrderBy(at => at.Id2).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' ORDER BY 'id' ASC;");
        }

        [TestMethod]
        public void OrderByThenByDescending()
        {
            QueryFunc query = context => context.Values.OrderBy(v => v.Id).ThenByDescending(v2 => v2.Value).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' ORDER BY 'id' ASC,'value' DESC;");
        }

        [TestMethod]
        public void OrderByThenOrderBy()
        {
            QueryFunc query = context => context.Values.OrderBy(v => v.Id).OrderByDescending(v2 => v2.Value).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' ORDER BY 'id' ASC,'value' DESC;");
        }

        [TestMethod]
        [ExpectedException(typeof (CqlLinqException))]
        public void TakeBeforeOrderBy()
        {
            QueryFunc query = context => context.Values.Take(4).OrderBy(v => v.Id).ToList();
            ExecuteQuery(query, "invalid");
        }

        [TestMethod]
        public void OrderByThenTake()
        {
            QueryFunc query = context => context.Values.OrderBy(v => v.Id).Take(4).ToList();
            ExecuteQuery(query, "SELECT 'id','value' FROM 'myvalue' ORDER BY 'id' ASC LIMIT 4;");
        }

        #region Nested type: MyContext

        /// <summary>
        ///   The context used for testing
        /// </summary>
        public class MyContext : CqlContext
        {
            public CqlTable<MyValue> Values { get; set; }
        }

        #endregion

        #region Nested type: MyValue

        /// <summary>
        ///   class representing the values in a table
        /// </summary>
        public class MyValue
        {
            public int Id { get; set; }
            public string Value { get; set; }
        }

        #endregion

        #region Nested type: QueryFunc

        private delegate object QueryFunc(MyContext context);

        #endregion
    }
}