package simpledb.buffer;

import org.junit.Before;
import org.junit.Test;
import simpledb.file.Block;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class BufferMgrTest {

    private BufferMgr bufferMgr;
    private IBufferMgr iBufferMgr;

    @Before
    public void setUp(){
        iBufferMgr = mock(IBufferMgr.class);
        bufferMgr = new BufferMgr(iBufferMgr );
    }

    @Test(expected = BufferAbortException.class)
    public void exceptionIsThrownIfNoBufferFoundToPin(){
        when(iBufferMgr.pin(any(Block.class))).thenReturn(null);
        bufferMgr.pin(mock(Block.class));
    }

    @Test(expected = BufferAbortException.class)
    public void exceptionIsThrownIfNoBufferFoundForNewPin(){
        when(iBufferMgr.pinNew(anyString(), any(PageFormatter.class))).thenReturn(null);
        bufferMgr.pinNew("somefile", mock(PageFormatter.class));
    }

    @Test
    public void unpinOfIBufferMgrIsCalled(){
        Buffer buff = new Buffer(1);
        bufferMgr.unpin(new Buffer(1));
        verify(iBufferMgr, times(1)).unpin(buff);
    }


}
