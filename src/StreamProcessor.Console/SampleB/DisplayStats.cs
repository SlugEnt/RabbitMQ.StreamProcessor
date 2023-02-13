using Spectre.Console;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace StreamProcessor.ConsoleScr.SampleB
{
    public class DisplayStats
    {
        private Table _table;
        //private Action<LiveDisplayContext> lvdContextAction;
        //private LiveDisplay liveDisplay;
        private LiveDisplayContext _liveDisplayContext;
        private List<Stats> _stats;


        public DisplayStats(List<Stats> Stats)
        {
            _stats = Stats;

            _table = new Table().Centered();

            BuildColumns();
            AddRows();
        }



        private void BuildColumns()
        {
            // Columns
            _table.ShowHeaders = true;
            _table.AddColumn("Title");
            _table.Columns[0].PadRight(6);
            foreach (Stats stat in _stats)
            {
                _table.AddColumn(stat.Name);
            }
        }

        private void AddRows()
        {
            _table.AddRow(MarkUpValue("Msg Sent", "green"));
            _table.AddRow(MarkUpValue("Created Msg", "green"));
            _table.AddRow(MarkUpValue("Success Msg", "green"));
            _table.AddRow(MarkUpValue("Failure Msg", "green"));
            _table.AddRow(MarkUpValue("CB Tripped", "green"));
            _table.AddRow(MarkUpValue("Consumed Msg", "green"));

        }

        private void AddColumnsForStats()
        {
            int col = 0;
            foreach (Stats stat in _stats)
            {
                col++;
                _table.UpdateCell(0, col, MarkUp(stat.SuccessMessages));
                _table.UpdateCell(1, col, MarkUp(stat.CreatedMessages));
                _table.UpdateCell(2, col, MarkUp(stat.SuccessMessages));
                _table.UpdateCell(3, col, MarkUp(stat.FailureMessages,false));
                _table.UpdateCell(4, col, MarkUp(stat.CircuitBreakerTripped));

            }
        }

        private string MarkUpValue(string value, string colorName, bool bold = false, bool underline = false,
            bool italic = false)
        {
            string val = "[" + colorName + "]";
            if (bold) val += "[bold]";


            val += value + "[/]";
            return val;
        }


        private string MarkUp(ulong value, bool positiveGreen = true)
        {
            string color = "green";
            if (!positiveGreen)
            {
                if (value > 0) color = "red";
            } 

            string val = "[" + color + "]";
            val += value + "[/]";
            return val;

        }


        private string MarkUp(bool value, bool trueGreen = true)
        {
            string color = "";
            if (trueGreen) color = "green";
            else color = "red";

            string val = "[" + color + "]";
            val += value + "[/]";
            return val;

        }

        private void SaveContext(LiveDisplayContext liveDisplayContext)
        {

            _liveDisplayContext = liveDisplayContext;
            _liveDisplayContext.Refresh();
        }

        public void Refresh()
        {
            System.Console.Clear();
            AddColumnsForStats();
            AnsiConsole.Write(_table);
            //    _liveDisplayContext.Refresh();
        }
    }
}
