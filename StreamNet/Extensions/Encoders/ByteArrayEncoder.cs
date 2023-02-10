using System.Text;
using Newtonsoft.Json;

namespace StreamNet.Extensions.Encoders
{
    internal static class ByteArrayEncoder
    {
        internal static byte[] EncodeToByteArray<T>(T message) => Encoding.ASCII.GetBytes(JsonConvert.SerializeObject(message));
    }
}