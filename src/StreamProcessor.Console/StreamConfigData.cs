using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor.Console
{
    internal class StreamConfigData
    {
        public string Name { get; set; }
        public long CheckPointOffset { get; set; }
    }
}
