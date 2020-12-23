package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.fs.impl.FutureIOSupport.awaitFuture;

public class ListStatusRemoteIterator implements RemoteIterator<FileStatus> {

  private static final boolean FETCH_ALL_FALSE = false;

  private final Path path;
  private final AzureBlobFileSystemStore abfsStore;

  private boolean firstRead = true;
  private String continuation;
  private CompletableFuture<IOException> future;
  private ListIterator<FileStatus> primaryListIterator;
  private ListIterator<FileStatus> secondaryListIterator;

  public ListStatusRemoteIterator(final Path path,
      final AzureBlobFileSystemStore abfsStore) throws IOException {
    this.path = path;
    this.abfsStore = abfsStore;
    fetchMoreFileStatuses();
    forceCurrentFuture();
    fetchMoreFileStatuses();
    forceCurrentFuture();
    secondaryListIterator=null;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (primaryListIterator.hasNext()) {
      return true;
    }
    if (secondaryListIterator == null) {
      return false;
    }
    fetchMoreFileStatuses();
    return primaryListIterator.hasNext();
  }

  @Override
  public FileStatus next() throws IOException {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }
    return primaryListIterator.next();
  }

  private void fetchMoreFileStatuses() throws IOException {
    primaryListIterator = secondaryListIterator;
    forceCurrentFuture();
    if (!isIterationComplete()) {
      fetchMoreFileStatusesAsync();
    } else {
      primaryListIterator = null;
    }
  }

  private boolean isIterationComplete() {
    return !firstRead && (continuation == null || continuation.isEmpty());
  }

  private void forceCurrentFuture() throws IOException {
    if (future == null) {
      return;
    }
    IOException ex = awaitFuture(future);
    if (ex != null) {
      throw ex;
    }
    future = null;
  }

  private void fetchMoreFileStatusesAsync() {
    future = CompletableFuture.supplyAsync(() -> {
      try {
        List<FileStatus> fileStatuses = new ArrayList<>();
        continuation = abfsStore
            .listStatus(path, null, fileStatuses, FETCH_ALL_FALSE,
                continuation);
        if (fileStatuses.isEmpty()) {
          secondaryListIterator = null;
        } else {
          secondaryListIterator = fileStatuses.listIterator();
        }
        if (firstRead) {
          firstRead = false;
        }
        return null;
      } catch (IOException ex) {
        return ex;
      }
    });
  }

}
