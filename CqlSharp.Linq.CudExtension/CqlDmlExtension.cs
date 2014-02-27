using CqlSharp.Linq.CudExtension.TypeHelpers;
using System;
using System.Collections;
using System.Collections.Specialized;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using CqlSharp.Linq.CudExtension;

namespace CqlSharp.Linq
{
    public enum SubmitAction
    {
        None,
        Update,
        PossibleUpdate,
        Insert,
        InsertOrUpdate,
        Delete
    }

    internal class TrackedTable<T>
    {
        public TrackedTable(CqlTable<T> table, T instance, SubmitAction submitAction)
        {
            CqlContext cqlContext = (CqlContext)table.Provider;

            if (cqlContext.TrackedTables == null)
                cqlContext.TrackedTables = new HybridDictionary();

            string tableName = table.Name;

            Queue<TrackedItem> trackedItemsQueue = new Queue<TrackedItem>();
            if (cqlContext.TrackedTables.Contains(tableName))
                trackedItemsQueue = (Queue<TrackedItem>)cqlContext.TrackedTables[tableName];

            TrackedItem newTrackedItem = new TrackedItem(tableName, instance, submitAction);
            trackedItemsQueue.Enqueue(newTrackedItem);

            cqlContext.TrackedTables[tableName] = trackedItemsQueue;
        }
    }

    internal class TrackedItem
    {
        public string Table { get; set; }
        public SubmitAction State { get; set; }
        public Dictionary<string, bool> PrimaryKeys { get; set; }
        public Dictionary<string, object> InstanceProps { get; set; }

        internal TrackedItem(string table, object instance, SubmitAction state)
        {
            Table = table;
            State = state;
            InstanceProps = TypeProperties.GetTypeProperties(instance);
            PrimaryKeys = TypeProperties.GetPrimaryKeys(instance);
        }
    }

    public static class CqlDmlExtension
    {
        /// <summary>
        /// Inserts the records on submit.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="instance">The instance.</param>
        /// <exception cref="System.ArgumentNullException">instance</exception>
        public static void InsertOnSubmit<T>(this CqlTable<T> table, T instance)
        {
            if (instance == null)
                throw new ArgumentNullException("instance");

            TrackedTable<T> trackedTable = new TrackedTable<T>(table, instance, SubmitAction.Insert);
        }

        /// <summary>
        /// Adds the object to be updated into the tracked items dictionary.
        /// The standard LINQ and Entity providers do not have explicit UpdateOnSubmit but they 
        /// track the object changes automatically.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table"></param>
        /// <param name="instance"></param>
        /// <remarks>This can be eliminated after implementing OnPropertyChanged support for updates</remarks>
        public static void UpdateOnSubmit<T>(this CqlTable<T> table, T instance)
        {
            if (instance == null)
                throw new ArgumentNullException("instance");

            TrackedTable<T> trackedTable = new TrackedTable<T>(table, instance, SubmitAction.Update);
        }

        /// <summary>
        /// Deletes the records marked for deletion, on submit.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="table">The table.</param>
        /// <param name="instance">The instance.</param>
        /// <exception cref="System.ArgumentNullException">instance</exception>
        public static void DeleteOnSubmit<T>(this CqlTable<T> table, T instance)
        {
            if (instance == null)
                throw new ArgumentNullException("instance");

            TrackedTable<T> trackedTable = new TrackedTable<T>(table, instance, SubmitAction.Delete);
        }

        /// <summary>
        /// Submits the changes.
        /// </summary>
        /// <param name="cqlContext">The CQL context.</param>
        public static void SubmitChanges(this CqlContext cqlContext)
        {
            CqlDmlBuilder cqlDmlBuilder = new CqlDmlBuilder();
            if (cqlContext.TrackedTables == null || cqlContext.TrackedTables.Count < 1)
            {
                Debug.WriteLine("SubmitChanges: No records to apply changes");
                return;
            }

            ICollection trackedTableKeys = cqlContext.TrackedTables.Keys;
            CqlDmlExecutor cqlDmlExectutor = new CqlDmlExecutor(cqlContext.ConnectionString);
            try
            {
                foreach (object trackedTableKey in trackedTableKeys)
                {
                    Queue<TrackedItem> trackedItemsKey = (Queue<TrackedItem>)cqlContext.TrackedTables[trackedTableKey];
                    TrackedItem trackedItem = null;
                    while (trackedItemsKey != null && trackedItemsKey.Count > 0)
                    {
                        trackedItem = trackedItemsKey.Dequeue();
                        var cql = new CqlDmlBuilder().BuildDmlQuery((string)trackedTableKey, trackedItem);
                        Debug.WriteLine("Generated CQL (DML): " + cql);

                        int recsChanged = cqlDmlExectutor.ExecuteNonQuery(cql);
                        Debug.WriteLine(string.Format("Records updated {0}", recsChanged));
                    }                    
                }

                Debug.WriteLine("Submitted all changes successfully");
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                cqlDmlExectutor.CloseConnection();
                throw;
            }
            finally
            {
                cqlContext.TrackedTables.Clear();
            }
            cqlDmlExectutor.CloseConnection();
        }
    }
}