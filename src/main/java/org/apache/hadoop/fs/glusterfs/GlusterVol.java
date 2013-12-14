/**
 *
 * Copyright (c) 2013 Red Hat, Inc. <http://www.redhat.com>
 * This file is part of GlusterFS.
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * Extends the RawLocalFileSystem to add support for Gluster Volumes. 
 * 
 */

package org.apache.hadoop.fs.glusterfs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.HasFileDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.gluster.fs.GlusterClient;
import org.gluster.fs.GlusterFile;
import org.gluster.fs.GlusterInputStream;
import org.gluster.fs.GlusterOutputStream;
import org.gluster.fs.GlusterVolume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlusterVol extends FileSystem {

	static final URI NAME = URI.create("glusterfs:///");
	private Path workingDir;

	public GlusterVol() {}
	
	public GlusterVolume vol;

	private Path makeAbsolute(Path f) {
		if (f.isAbsolute()) {
			return f;
		} else {
			return new Path(workingDir, f);
		}
	}

	public URI getUri() {
		return NAME;
	}

	public String pathOnly(Path path){
		return path.toUri().getPath();
	}
	
	@Override
	public void initialize(URI uri, Configuration conf) throws IOException {
		super.initialize(uri, conf);
		setConf(conf);
	}

	class TrackingInputStreamWrapper extends InputStream {
		
		GlusterInputStream ios = null;
		
		public TrackingInputStreamWrapper(GlusterInputStream ios) throws IOException {
			this.ios = ios;
		}
		
		public GlusterInputStream getChannel(){
			return this.ios;
		}
		
		public int read() throws IOException {
			int result = ios.read();
			if (result != -1) {
				statistics.incrementBytesRead(1);
			}
			return result;
		}

		public int read(byte[] data) throws IOException {
			int result = ios.read(data);
			if (result != -1) {
				statistics.incrementBytesRead(result);
			}
			return result;
		}

		@Override
		public int read(byte[] data, int offset, int length) throws IOException {
			int result = ios.read(data, offset, length);
			if (result != -1) {
				statistics.incrementBytesRead(result);
			}
			return result;
		}
	}

	/*******************************************************
	 * For open()'s FSInputStream.
	 *******************************************************/
	class GlussterFileInputStream extends FSInputStream  {
		private TrackingInputStreamWrapper fis;
	
		public GlussterFileInputStream(Path f) throws IOException {
			GlusterInputStream gis = vol.open(pathOnly(f)).inputStream();
			this.fis = new TrackingInputStreamWrapper(gis);
		}

		public void seek(long pos) throws IOException {
			fis.getChannel().seek(new Long(pos).intValue());
			
		}

		public long getPos() throws IOException {
			return fis.getChannel().offset();
		}

		public boolean seekToNewSource(long targetPos) throws IOException {
			this.seek(targetPos);
			return true;
		}

		/*
		 * Just forward to the fis
		 */
		@Override
		public int available() throws IOException {
			return fis.available();
		}

		@Override
		public void close() throws IOException {
			fis.close();
		}

		@Override
		public boolean markSupported() {
			return fis.getChannel().markSupported();
		}

		@Override
		public int read() throws IOException {
				return fis.read();
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
				return fis.read(b, off, len);
		}

		@Override
		public int read(long position, byte[] b, int off, int len) throws IOException {
		     return fis.getChannel().read(b, off, len);
		}

		@Override
		public long skip(long n) throws IOException {
			return  fis.skip(n);
		}

	}

	@Override
	public FSDataInputStream open(Path f, int bufferSize) throws IOException {
		if (!exists(f)) {
			throw new FileNotFoundException(f.toString());
		}
		return new FSDataInputStream(new BufferedFSInputStream(new GlussterFileInputStream(f), bufferSize));
	}

	/*********************************************************
	 * For create()'s FSOutputStream.
	 *********************************************************/
	class GlusterFileOutputStream extends OutputStream {
		private GlusterOutputStream fos;

		private GlusterFileOutputStream(Path f, boolean append)	throws IOException {
			
			// always allow append 
			this.fos = vol.open(pathOnly(f)).outputStream();
		}

		@Override
		public void close() throws IOException {
			fos.close();
		}

		@Override
		public void flush() throws IOException {
			fos.flush();
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			fos.write(b, off, len);
		}

		@Override
		public void write(int b) throws IOException {
				fos.write(b);
		}
	}

	@Override
	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
		if (!exists(f)) {
			throw new FileNotFoundException("File " + f + " not found");
		}
		if (getFileStatus(f).isDirectory()) {
			throw new IOException("Cannot append to a diretory (=" + f + " )");
		}
		return new FSDataOutputStream(new BufferedOutputStream(new GlusterFileOutputStream(f, true), bufferSize), statistics);
	}

	@Override
	public FSDataOutputStream create(Path f, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress)
			throws IOException {
		return create(f, overwrite, true, bufferSize, replication, blockSize,
				progress);
	}

	private FSDataOutputStream create(Path f, boolean overwrite,
			boolean createParent, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {
		if (exists(f) && !overwrite) {
			throw new IOException("File already exists: " + f);
		}
		Path parent = f.getParent();
		if (parent != null && !mkdirs(parent)) {
			throw new IOException("Mkdirs failed to create "
					+ parent.toString());
		}
		return new FSDataOutputStream(new BufferedOutputStream(
				new GlusterFileOutputStream(f, false), bufferSize), statistics);
	}

	
	
	public FSDataOutputStream createNonRecursive(Path f,FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
		if (exists(f) && !flags.contains(CreateFlag.OVERWRITE)) {
			throw new IOException("File already exists: " + f);
		}
		return new FSDataOutputStream(new BufferedOutputStream(
				new GlusterFileOutputStream(f, false), bufferSize), statistics);
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication,
			long blockSize, Progressable progress) throws IOException {

		FSDataOutputStream out = create(f, overwrite, bufferSize, replication,
				blockSize, progress);
		setPermission(f, permission);
		return out;
	}

	@Override
	public FSDataOutputStream createNonRecursive(Path f,
			FsPermission permission, boolean overwrite, int bufferSize,
			short replication, long blockSize, Progressable progress)
			throws IOException {
		FSDataOutputStream out = create(f, overwrite, false, bufferSize,
				replication, blockSize, progress);
		setPermission(f, permission);
		return out;
	}

	@Override
	public boolean rename(Path src, Path dst) throws IOException {
		return (vol.open(pathOnly(src)).renameTo(vol.open(pathOnly(dst)))); 
			
	}

	/**
	 * Delete the given path to a file or directory.
	 * 
	 * @param p
	 *            the path to delete
	 * @param recursive
	 *            to delete sub-directories
	 * @return true if the file or directory and all its contents were deleted
	 * @throws IOException
	 *             if p is non-empty and recursive is false
	 */
	@Override
	public boolean delete(Path p, boolean recursive) throws IOException {
		GlusterFile file = vol.open(pathOnly(p));
		return file.delete(recursive);
	}

	@Override
	public FileStatus[] listStatus(Path f) throws IOException {
		File localf = pathToFile(f);
		FileStatus[] results;

		if (!localf.exists()) {
			throw new FileNotFoundException("File " + f + " does not exist");
		}
		if (localf.isFile()) {
			return new FileStatus[] { new RawLocalFileStatus(localf,
					getDefaultBlockSize(f), this) };
		}

		File[] names = localf.listFiles();
		if (names == null) {
			return null;
		}
		results = new FileStatus[names.length];
		int j = 0;
		for (int i = 0; i < names.length; i++) {
			try {
				results[j] = getFileStatus(new Path(names[i].getAbsolutePath()));
				j++;
			} catch (FileNotFoundException e) {
				// ignore the files not found since the dir list may have have
				// changed
				// since the names[] list was generated.
			}
		}
		if (j == names.length) {
			return results;
		}
		return Arrays.copyOf(results, j);
	}

	/**
	 * Creates the specified directory hierarchy. Does not treat existence as an
	 * error.
	 */
	@Override
	public boolean mkdirs(Path f) throws IOException {
		if (f == null) {
			throw new IllegalArgumentException("mkdirs path arg is null");
		}
		Path parent = f.getParent();
		File p2f = pathToFile(f);
		if (parent != null) {
			File parent2f = pathToFile(parent);
			if (parent2f != null && parent2f.exists()
					&& !parent2f.isDirectory()) {
				throw new FileAlreadyExistsException(
						"Parent path is not a directory: " + parent);
			}
		}
		return (parent == null || mkdirs(parent))
				&& (p2f.mkdir() || p2f.isDirectory());
	}

	@Override
	public boolean mkdirs(Path f, FsPermission permission) throws IOException {
		boolean b = mkdirs(f);
		if (b) {
			setPermission(f, permission);
		}
		return b;
	}

	@Override
	protected boolean primitiveMkdir(Path f, FsPermission absolutePermission)
			throws IOException {
		boolean b = mkdirs(f);
		setPermission(f, absolutePermission);
		return b;
	}

	@Override
	public Path getHomeDirectory() {
		return this.makeQualified(new Path(System.getProperty("user.home")));
	}

	/**
	 * Set the working directory to the given directory.
	 */
	@Override
	public void setWorkingDirectory(Path newDir) {
		workingDir = makeAbsolute(newDir);
		checkPath(workingDir);

	}

	@Override
	public Path getWorkingDirectory() {
		return workingDir;
	}

	@Override
	protected Path getInitialWorkingDirectory() {
		return this.makeQualified(new Path(System.getProperty("user.dir")));
	}

	@Override
	public FsStatus getStatus(Path p) throws IOException {
		File partition = pathToFile(p == null ? new Path("/") : p);
		// File provides getUsableSpace() and getFreeSpace()
		// File provides no API to obtain used space, assume used = total - free
		return new FsStatus(partition.getTotalSpace(),
				partition.getTotalSpace() - partition.getFreeSpace(),
				partition.getFreeSpace());
	}

	// In the case of the local filesystem, we can just rename the file.
	@Override
	public void moveFromLocalFile(Path src, Path dst) throws IOException {
		rename(src, dst);
	}

	// We can write output directly to the final location
	@Override
	public Path startLocalOutput(Path fsOutputFile, Path tmpLocalFile)
			throws IOException {
		return fsOutputFile;
	}

	// It's in the right place - nothing to do.
	@Override
	public void completeLocalOutput(Path fsWorkingFile, Path tmpLocalFile)
			throws IOException {
	}

	@Override
	public void close() throws IOException {
		super.close();
	}

	@Override
	public String toString() {
		return "LocalFS";
	}

	@Override
	public FileStatus getFileStatus(Path f) throws IOException {
		File path = pathToFile(f);
		if (path.exists()) {
			return new RawLocalFileStatus(pathToFile(f),
					getDefaultBlockSize(f), this);
		} else {
			throw new FileNotFoundException("File " + f + " does not exist");
		}
	}

	static class RawLocalFileStatus extends FileStatus {
		/*
		 * We can add extra fields here. It breaks at least
		 * CopyFiles.FilePair(). We recognize if the information is already
		 * loaded by check if onwer.equals("").
		 */
		private boolean isPermissionLoaded() {
			return !super.getOwner().equals("");
		}

		RawLocalFileStatus(File f, long defaultBlockSize, FileSystem fs) {
			super(f.length(), f.isDirectory(), 1, defaultBlockSize, f
					.lastModified(), fs.makeQualified(new Path(f.getPath())));
		}

		@Override
		public FsPermission getPermission() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getPermission();
		}

		@Override
		public String getOwner() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getOwner();
		}

		@Override
		public String getGroup() {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			return super.getGroup();
		}

		// / loads permissions, owner, and group from `ls -ld`
		private void loadPermissionInfo() {
			IOException e = null;
			try {
				StringTokenizer t = new StringTokenizer(execCommand(new File(
						getPath().toUri()), Shell.getGET_PERMISSION_COMMAND()));
				// expected format
				// -rw------- 1 username groupname ...
				String permission = t.nextToken();
				if (permission.length() > 10) { // files with ACLs might have a
												// '+'
					permission = permission.substring(0, 10);
				}
				setPermission(FsPermission.valueOf(permission));
				t.nextToken();
				setOwner(t.nextToken());
				setGroup(t.nextToken());
			} catch (Shell.ExitCodeException ioe) {
				if (ioe.getExitCode() != 1) {
					e = ioe;
				} else {
					setPermission(null);
					setOwner(null);
					setGroup(null);
				}
			} catch (IOException ioe) {
				e = ioe;
			} finally {
				if (e != null) {
					throw new RuntimeException(
							"Error while running command to get "
									+ "file permissions : "
									+ StringUtils.stringifyException(e));
				}
			}
		}

		@Override
		public void write(DataOutput out) throws IOException {
			if (!isPermissionLoaded()) {
				loadPermissionInfo();
			}
			super.write(out);
		}
	}

	/**
	 * Use the command chown to set owner.
	 */
	@Override
	public void setOwner(Path p, String username, String groupname)
			throws IOException {
		if (username == null && groupname == null) {
			throw new IOException("username == null && groupname == null");
		}

		if (username == null) {
			execCommand(pathToFile(p), Shell.SET_GROUP_COMMAND, groupname);
		} else {
			// OWNER[:[GROUP]]
			String s = username + (groupname == null ? "" : ":" + groupname);
			execCommand(pathToFile(p), Shell.SET_OWNER_COMMAND, s);
		}
	}

	/**
	 * Use the command chmod to set permission.
	 */
	@Override
	public void setPermission(Path p, FsPermission permission)
			throws IOException {
		if (NativeIO.isAvailable()) {
			NativeIO.chmod(pathToFile(p).getCanonicalPath(),
					permission.toShort());
		} else {
			execCommand(pathToFile(p), Shell.SET_PERMISSION_COMMAND,
					String.format("%05o", permission.toShort()));
		}
	}

	private static String execCommand(File f, String... cmd) throws IOException {
		String[] args = new String[cmd.length + 1];
		System.arraycopy(cmd, 0, args, 0, cmd.length);
		args[cmd.length] = FileUtil.makeShellPath(f, true);
		String output = Shell.execCommand(args);
		return output;
	}


}
