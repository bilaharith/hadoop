package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.azurebfs.AzureBlobFileSystemStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.hadoop.fs.impl.FutureIOSupport.awaitFuture;

public class ListStatusRemoteIterator implements RemoteIterator<FileStatus> {

  private static final boolean FETCH_ALL_FALSE = false;

  private final Path path;
  private final AzureBlobFileSystemStore abfsStore;

  private CompletableFuture<IOException> future;
  private final Queue<ListIterator<FileStatus>> iterators = new LinkedList<>();

  int c = 0;

  private boolean firstRead = true;
  private String continuation;
  private ListIterator<FileStatus> lsItr;

  public ListStatusRemoteIterator(final Path path,
      final AzureBlobFileSystemStore abfsStore) throws IOException {
    this.path = path;
    this.abfsStore = abfsStore;
    fetchMoreFileStatusesAsync();
    forceFuture();
    fetchMoreFileStatusesAsync();
    forceFuture();
    lsItr = iterators.poll();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (lsItr.hasNext()) {
      return true;
    }
    System.out.println("Fetching more");
    fetchMoreFileStatusesAsync();
    if (!iterators.isEmpty()) {
      System.out.println("iterators not empty");
      lsItr = iterators.poll();
    } else if (future!=null) {
      System.out.println("futures not empty");
      forceFuture();
      lsItr = iterators.poll();
    }
    if (lsItr == null) {
      System.out.println("null iterator");
      return false;
    }
    return lsItr.hasNext();
  }

  @Override
  public FileStatus next() throws IOException {
    if (!this.hasNext()) {
      throw new NoSuchElementException();
    }
    return lsItr.next();
  }

  private void forceFuture() throws IOException {
    if (future==null) {
      System.out.println("futures is empty");
      return;
    }
    System.out.println("Forcng the future");
    IOException ex = awaitFuture(future);
    System.out.println("ForceFuture done ex: " + ex);
    if (ex != null) {
      throw ex;
    }
  }

  private void fetchMoreFileStatusesAsync() throws IOException {
    c++;
    System.out.println("fetch: " + c);
    forceFuture();
    if (isIterationComplete()) {
      System.out.println("Iteration complete");
      return;
    }
    this.future = CompletableFuture
        .supplyAsync(() -> {
          synchronized (this) {
            try {
              List<FileStatus> fileStatuses = new ArrayList<>();
              continuation = abfsStore
                  .listStatus(path, null, fileStatuses, FETCH_ALL_FALSE,
                      continuation);
              if (fileStatuses != null && !fileStatuses.isEmpty()) {
                System.out.println("Adding to iterators");
                iterators.add(fileStatuses.listIterator());
              }
              if (firstRead) {
                firstRead = false;
              }
              return null;
            } catch (IOException ex) {
              return ex;
            }
          }
        });
  }

  private boolean isIterationComplete() {
    return !firstRead && (continuation == null || continuation.isEmpty());
  }
  
}
