using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StreamProcessor.Console
{
    internal class Config
    {
        public Dictionary<string,StreamConfigData> Streams = new Dictionary<string, StreamConfigData>();

        public Config() { }

        public void UpdateStreamData(StreamConfigData streamConfigData)
        {
            if (Streams.ContainsKey(streamConfigData.Name))
            {
                Streams[streamConfigData.Name] = streamConfigData;
            }
            else
            {
                Streams.Add(streamConfigData.Name, streamConfigData);
            }
        }
    }
}
