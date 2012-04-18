using System;
using System.Linq.Expressions;

namespace Raptile.Indices
{
    public class IndexExtractor<T>
    {
        private Func<T, object> _extractor;

        public IndexExtractor(Expression<Func<T, object>> definition)
        {
            Translate(definition);
        }

        private void Translate(Expression<Func<T, object>> definition)
        {
            _extractor = definition.Compile();
        }

        public string Read(object obj)
        {
            if (!(obj is T))
                return null;
            var o = _extractor((T)obj);
            return o.ToString();
        }
    }
}