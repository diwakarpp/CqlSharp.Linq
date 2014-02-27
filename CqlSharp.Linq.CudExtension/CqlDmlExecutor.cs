using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using CqlSharp;

namespace CqlSharp.Linq.CudExtension
{
    internal class CqlDmlExecutor
    {
        private string _connectionString;
        private CqlConnection _cqlConnection;
        /// <summary>
        /// Initializes a new instance of the <see cref="CqlDmlExecutor"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public CqlDmlExecutor(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        public void OpenConnection()
        {
            _cqlConnection = new CqlConnection(_connectionString);
            _cqlConnection.Open();
        }

        /// <summary>
        /// Executes the non query.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="cqlParamsCollection">The CQL parameters collection.</param>
        /// <returns></returns>
        public int ExecuteNonQuery(string command, CqlParameterCollection cqlParamsCollection = null)
        {
            if (_cqlConnection == null)
                OpenConnection();

            if (_cqlConnection.State == ConnectionState.Closed || _cqlConnection.State == ConnectionState.Broken)
            {
                CloseConnection();
                OpenConnection();
            }

            if (!string.IsNullOrWhiteSpace(command))
            {
                CqlCommand cqlCommand = new CqlCommand(_cqlConnection, command);
                if (cqlParamsCollection != null)
                {
                    foreach (CqlParameter cqlParam in cqlParamsCollection)
                        cqlCommand.Parameters.Add(cqlParam);
                }

                int nonQueryRet = cqlCommand.ExecuteNonQuery();
                return nonQueryRet;
            }

            return -1;
        }

        /// <summary>
        /// Closes the connection.
        /// </summary>
        public void CloseConnection()
        {
            if (_cqlConnection == null)
                return;

            _cqlConnection.Close();
            if (_cqlConnection != null)
            {
                _cqlConnection.Dispose();
                _cqlConnection = null;
            }
        }

        ~CqlDmlExecutor()
        {
            CloseConnection();
        }
    }
}
