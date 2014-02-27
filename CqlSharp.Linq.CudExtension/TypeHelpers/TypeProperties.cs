using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CqlSharp.Linq.CudExtension.TypeHelpers
{
    public class TypeProperties
    {
        /// <summary>
        /// Gets the type properties.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public static Dictionary<string, object> GetTypeProperties(object instance)
        {
            Dictionary<string, object> typeProperties = new Dictionary<string, object>();
            if (instance != null)
            {
                Type instanceType = instance.GetType();
                typeProperties = GetTypeProperties(instanceType, instance);
            }
            return typeProperties;
        }

        /// <summary>
        /// Gets the primary keys.
        /// </summary>
        /// <param name="instance">The instance.</param>
        /// <returns></returns>
        public static Dictionary<string, bool> GetPrimaryKeys(object instance)
        {
            Dictionary<string, bool> primaryKeys = new Dictionary<string, bool>();
            if (instance != null)
            {
                Type instanceType = instance.GetType();
                primaryKeys = GetPrimaryKeys(instanceType, instance);
            }
            return primaryKeys;
        }

        private static Dictionary<string, object> GetTypeProperties(Type instanceType, object instance)
        {
            Dictionary<string, object> propertiesDict = new Dictionary<string, object>();
            foreach (PropertyInfo p in instanceType.GetProperties())
            {
                Type propertyType = p.PropertyType;
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    propertyType = Nullable.GetUnderlyingType(propertyType);

                propertiesDict[p.Name] = p.GetValue(instance);
            }
            return propertiesDict;
        }

        private static Dictionary<string, bool> GetPrimaryKeys(Type instanceType, object instance)
        {
            Dictionary<string, bool> primaryKeys = new Dictionary<string, bool>();
            foreach (PropertyInfo p in instanceType.GetProperties())
            {
                Type propertyType = p.PropertyType;
                if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    propertyType = Nullable.GetUnderlyingType(propertyType);

                object[] attrs = p.GetCustomAttributes(true);
                foreach (object attr in attrs)
                {
                    System.Data.Linq.Mapping.ColumnAttribute primaryKeyAttr = attr as System.Data.Linq.Mapping.ColumnAttribute;
                    if (primaryKeyAttr != null)
                    {
                        if (primaryKeyAttr.IsPrimaryKey)
                            primaryKeys[p.Name] = primaryKeyAttr.IsPrimaryKey;
                    }
                }
            }
            return primaryKeys;
        }
    }
}
