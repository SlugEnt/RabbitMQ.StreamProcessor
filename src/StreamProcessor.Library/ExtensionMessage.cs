using System.Net.NetworkInformation;
using System.Text;
using RabbitMQ.Stream.Client;

namespace SlugEnt.MQStreamProcessor;

public static class ExtensionMessage
{
    /// <summary>
    /// Returns the object from the UTF8 message text
    /// </summary>
    /// <typeparam name="T">Type of object in the message</typeparam>
    /// <param name="message">The message</param>
    /// <returns></returns>
    public static T GetObject<T>(this Message message)
    {
        string json  = Encoding.UTF8.GetString(message.Data.Contents);
        T      value = System.Text.Json.JsonSerializer.Deserialize<T>(json);
        return value;
    }



    /// <summary>
    /// Retrieves the UTF8 Encoded text of the message
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public static string GetText(this Message message) { return Encoding.UTF8.GetString(message.Data.Contents); }


    /// <summary>
    /// Adds the given property to the Application Property Dictionary
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of the property</param>
    /// <param name="propertyValue">Value of the property (as string)</param>
    public static void AddApplicationProperty(this Message message, string propertyName, string propertyValue)
    {
        message.ApplicationProperties.Add(propertyName, propertyValue);
    }


    /// <summary>
    /// Adds the given property to the Application Property Dictionary
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of the property</param>
    /// <param name="value">Value Object</param>
    public static void AddApplicationProperty<T>(this Message message, string propertyName, T value)
    {
        string json = System.Text.Json.JsonSerializer.Serialize(value);
        message.ApplicationProperties.Add(propertyName, json);
    }


    /// <summary>
    /// Attempts to retrieve the Application Property with the given name from the dictionary.  Returns String.Empty if not found
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of application property to retrieve</param>
    /// <returns></returns>
    public static string GetApplicationPropertyAsString(this Message message, string propertyName)
    {
        if (message.ApplicationProperties.TryGetValue(propertyName, out object value))
        {
            return value.ToString();
        }

        return string.Empty;
    }


    /// <summary>
    /// Attempts to retrieve the Application Property with the given name from the dictionary.  Returns default value for T object.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of application property to retrieve</param>
    /// <returns>Object T or Null if not found</returns>
    public static T GetApplicationProperty<T>(this Message message, string propertyName)
    {
        if (message.ApplicationProperties.TryGetValue(propertyName, out object value))
        {
            string json     = value.ToString();
            T      valueAsT = System.Text.Json.JsonSerializer.Deserialize<T>(json);
            return valueAsT;
        }

        return default(T);
    }


    /// <summary>
    /// Prints information about the message.  So key properties and ApplicationProperties
    /// </summary>
    /// <returns></returns>
    public static string PrintMessageInfo(this Message message)
    {
        StringBuilder sb = new StringBuilder();


        if (message.Properties.Subject != string.Empty)
            sb.Append($"\nProp: Subject: {message.Properties.Subject}");

        if (message.Properties.CreationTime != null)
            sb.Append($"\nProp: CreationTime: {message.Properties.CreationTime}");

        if (message.Properties.CorrelationId != null)
            sb.Append($"\nProp: CorrelationId: {message.Properties.CorrelationId.ToString()}");

        if (message.Properties.ContentType != String.Empty)
            sb.Append($"\nProp: ContentType: {message.Properties.ContentType}");

        foreach (KeyValuePair<string, object> appProperty in message.ApplicationProperties)
        {
            sb.Append($"\nAppProp:  {appProperty.Key} --> {appProperty.Value.ToString()}");
        }

        return sb.ToString();
    }
}