# This pulls in the canape library namespaces
import CANAPE.Nodes
import CANAPE.DataFrames
import re
from StringIO import StringIO
import base64
import gzip

# Simple pipeline node to mutate based on a regular expression
class ReMutatorNode(CANAPE.Nodes.BasePipelineNode):

    # Called when a new frame has arrived
    def OnInput(self, frame):
    
        stext = gzip.GzipFile(mode='rb', fileobj=StringIO(frame.ToByteString()))
        
        # Create a new data frame with the contents of the string
        self.WriteOutput(CANAPE.DataFrames.DataFrame(stext))
