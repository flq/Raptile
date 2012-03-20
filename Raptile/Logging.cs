using System;

namespace Raptile
{
    public interface ILog
    {
        void Debug(string msg, params object[] objs);
        void Error(string  msg, params object[] objs);
        void Info(string  msg, params object[] objs);
        void Warn(string  msg, params object[] objs);
        void Fatal(string  msg, params object[] objs);
    }

    internal class DebugLogger : ILog
    {
        private readonly string _name;

        public DebugLogger(Type type)
        {
            _name = type.Name;
        }

        public void Debug(string msg, params object[] objs)
        {
            WriteDebug("DEBUG", msg, objs);
        }

        private void WriteDebug(string level, string msg, object[] objs)
        {
            System.Diagnostics.Debug.WriteLine(string.Format("{0} ({1}):{2}", _name, level, string.Format(msg, objs)));
        }

        public void Error(string msg, params object[] objs)
        {
            WriteDebug("ERROR", msg, objs);
        }

        public void Info(string msg, params object[] objs)
        {
            WriteDebug("INFO", msg, objs);
        }

        public void Warn(string msg, params object[] objs)
        {
            WriteDebug("WARN", msg, objs);
        }

        public void Fatal(string msg, params object[] objs)
        {
            WriteDebug("FATAL", msg, objs);
        }
    }

    internal static class LogManager
    {
        public static Func<Type, ILog> LogFactory = t => new DebugLogger(t);

        public static ILog GetLogger(Type obj)
        {
            return LogFactory(obj);
        }
    }
}
