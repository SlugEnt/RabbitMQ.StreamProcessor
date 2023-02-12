using System.Text;
using System.Drawing;

namespace StreamProcessor.Console;

public static class HelperFunctions
{

    /// <summary>
    /// USed to create incrementing batches with letter naming.  So, A, Z, AAC, FRZ
    /// </summary>
    /// <param name="currentBatch"></param>
    /// <returns></returns>
    public static string NextBatch(string currentBatch)
    {
        byte z = (byte)'Z';
        byte[] batchIDs = Encoding.ASCII.GetBytes(currentBatch);

        int lastIndex = batchIDs.Length - 1;
        int currentIndex = lastIndex;
        bool continueLooping = true;

        while (continueLooping)
        {
            if (batchIDs[currentIndex] == z)
            {
                if (currentIndex == 0)
                {
                    // Append a new column
                    batchIDs[currentIndex] = (byte)'A';
                    string newBatch = Encoding.ASCII.GetString(batchIDs) + "A";
                    return newBatch;
                }

                // Change this index to A and move to the prior index.
                batchIDs[currentIndex] = (byte)'A';
                currentIndex--;
            }

            // Just increment this index to next letter
            else
            {
                batchIDs[currentIndex]++;
                return Encoding.ASCII.GetString(batchIDs);
            }
        }

        // Should never get here.
        return currentBatch;
    }



    public static void WriteInColor(string text, ConsoleColor foregroundColor)
    {
        var oldColor = System.Console.ForegroundColor;
        System.Console.ForegroundColor = foregroundColor;
        System.Console.WriteLine(text);
        System.Console.ForegroundColor = oldColor;
        
    }
}