using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Numerics;

namespace CqlSharp.Linq
{
    internal class CqlDmlBuilder
    {
        /// <summary>
        /// Builds the DML query.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="trackedItem">The tracked item.</param>
        /// <returns></returns>
        /// <exception cref="System.NotImplementedException">
        /// InsertOrUpdate is not yet implemented
        /// or
        /// PossibleUpdate is not yet implemented
        /// </exception>
        /// <exception cref="System.InvalidOperationException"></exception>
        public string BuildDmlQuery(string tableName, TrackedItem trackedItem)
        {
            switch (trackedItem.State)
            {
                case SubmitAction.Delete:
                    return BuildDeleteStatement(tableName, trackedItem);
                case SubmitAction.Insert:
                    return BuildInsertStatement(tableName, trackedItem);
                case SubmitAction.InsertOrUpdate:
                    throw new NotImplementedException("InsertOrUpdate is not yet implemented");
                case SubmitAction.PossibleUpdate:
                    throw new NotImplementedException("PossibleUpdate is not yet implemented");
                case SubmitAction.Update:
                    return BuildUpdateStatement(tableName, trackedItem);
                default:
                    throw new InvalidOperationException();
                    break;
            }
            return null;
        }

        #region Delete functions
        private string BuildDeleteStatement(string tableName, TrackedItem trackedItem)
        {
            StringBuilder deleteSb = new StringBuilder(string.Format("DELETE FROM \"{0}\" ", tableName.Replace("\"", "\"\"")));
            deleteSb.Append(" WHERE ");
            deleteSb.Append(TranslatePrimaryConditions(trackedItem.InstanceProps, trackedItem.PrimaryKeys));
            deleteSb.Append(";");

            return deleteSb.ToString();
        }
        #endregion

        #region Update functions
        private string BuildUpdateStatement(string tableName, TrackedItem trackedItem)
        {
            StringBuilder insertSb = new StringBuilder(string.Format("UPDATE \"{0}\" ", tableName.Replace("\"", "\"\"")));
            insertSb.Append(" SET ");
            insertSb.Append(TranslateUpdationIdValPairs(trackedItem.InstanceProps, trackedItem.PrimaryKeys));
            insertSb.Append(" WHERE ");
            insertSb.Append(TranslatePrimaryConditions(trackedItem.InstanceProps, trackedItem.PrimaryKeys));
            insertSb.Append(";");

            return insertSb.ToString();
        }

        private string TranslateUpdationIdValPairs(Dictionary<string, object> instanceProps, Dictionary<string, bool> primaryKeys)
        {
            StringBuilder identityValPairsSb = new StringBuilder();
            foreach (string identity in instanceProps.Keys)
            {
                //Exclude primary keys from the SET part
                if (primaryKeys.ContainsKey(identity))
                    continue;

                //Form the SET for rest of the identities
                if (identityValPairsSb.Length < 1)
                    identityValPairsSb.Append(string.Format("\"{0}\" = {1}", identity.Replace("\"", "\"\""), ToStringValue(instanceProps[identity])) );
                else
                    identityValPairsSb.Append(string.Format(", \"{0}\" = {1}", identity.Replace("\"", "\"\""), ToStringValue(instanceProps[identity])) );
            }

            return identityValPairsSb.ToString();
        }

        private string TranslatePrimaryConditions(Dictionary<string, object> instanceProps, Dictionary<string, bool> primaryKeys)
        {
            StringBuilder primaryKeyValPairsSb = new StringBuilder();
            foreach (string identity in primaryKeys.Keys)
            {
                if (primaryKeyValPairsSb.Length < 1)
                    primaryKeyValPairsSb.Append(string.Format("\"{0}\" = {1}", identity.Replace("\"", "\"\""), ToStringValue(instanceProps[identity])));
                else
                    primaryKeyValPairsSb.Append(string.Format(" AND \"{0}\" = {1}", identity.Replace("\"", "\"\""), ToStringValue(instanceProps[identity])));
            }

            return primaryKeyValPairsSb.ToString();
        }
        #endregion

        #region Insert functions
        private string BuildInsertStatement(string tableName, TrackedItem trackedItem)
        {
            StringBuilder insertSb = new StringBuilder(string.Format("INSERT INTO \"{0}\" ", tableName.Replace("\"", "\"\"")));
            insertSb.Append(" (");
            insertSb.Append(TranslateInsertionIds(trackedItem.InstanceProps));
            insertSb.Append(")");
            insertSb.Append(" VALUES ");
            insertSb.Append("(");
            insertSb.Append(TranslateInsertionIdValues(trackedItem.InstanceProps));
            insertSb.Append(");");

            return insertSb.ToString();
        }

        private string TranslateInsertionIds(Dictionary<string, object> instanceProps)
        {
            StringBuilder identitiesSb = new StringBuilder();
            foreach(string identity in instanceProps.Keys)
                identitiesSb.Append( identitiesSb.Length < 1 ? "\"" + identity.Replace("\"", "\"\"") + "\"" : ",\"" + identity.Replace("\"", "\"\"") + "\"");

            return identitiesSb.ToString();
        }

        private string TranslateInsertionIdValues(Dictionary<string, object> instanceProps)
        {
            StringBuilder identityValuesSb = new StringBuilder();
            foreach(string identity in instanceProps.Keys)
                identityValuesSb.Append( identityValuesSb.Length < 1 ?  ToStringValue(instanceProps[identity]) : ","+ ToStringValue(instanceProps[identity]));

            return identityValuesSb.ToString();
        }
        #endregion

        private string ToStringValue(object value)
        {
            Type type = value.GetType();
            switch (type.ToCqlType())
            {
                case CqlType.Text:
                case CqlType.Varchar:
                case CqlType.Ascii:
                    var str = (string)value;
                    return "'" + str.Replace("'", "''") + "'";

                case CqlType.Boolean:
                    return ((bool)value) ? "true" : "false";

                case CqlType.Decimal:
                case CqlType.Double:
                case CqlType.Float:
                    var culture = CultureInfo.InvariantCulture;
                    return string.Format(culture, "{0:E}", value);

                case CqlType.Counter:
                case CqlType.Bigint:
                case CqlType.Int:
                    return string.Format("{0:D}", value);

                case CqlType.Timeuuid:
                case CqlType.Uuid:
                    return ((Guid)value).ToString("D");

                case CqlType.Varint:
                    return ((BigInteger)value).ToString("D");

                case CqlType.Timestamp:
                    long timestamp = ((DateTime)value).ToTimestamp();
                    return string.Format("{0:D}", timestamp);

                case CqlType.Blob:
                    return ((byte[])value).ToHex("0x").Replace("-", "");

                default:
                    throw new CqlLinqException("Unable to translate term to a string representation");
            }
        }

        private string CleanUpCql(string cql)
        {
            return cql.Replace("\n", "").Replace("\r", "").Replace("\0", "").Trim();
        }
    }
}
