package org.apache.metamodel.spring;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.metamodel.util.Action;
import org.apache.metamodel.util.FileHelper;
import org.apache.metamodel.util.Func;
import org.apache.metamodel.util.Resource;
import org.apache.metamodel.util.ResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link Resource} implementation based on spring's similar
 * {@link org.springframework.core.io.Resource} concept.
 */
public class SpringResource implements Resource {

    private static final Logger logger = LoggerFactory.getLogger(SpringResource.class);

    private final org.springframework.core.io.Resource _resource;

    public SpringResource(org.springframework.core.io.Resource resource) {
        _resource = resource;
    }

    @Override
    public void append(Action<OutputStream> arg0) throws ResourceException {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLastModified() {
        try {
            return _resource.lastModified();
        } catch (IOException e) {
            logger.warn("Failed to get last modified date of resource: " + _resource, e);
            return -1;
        }
    }

    @Override
    public String getName() {
        return _resource.getFilename();
    }

    @Override
    public String getQualifiedPath() {
        try {
            return _resource.getURI().toString();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get URI of resource: " + _resource, e);
        }
    }

    @Override
    public long getSize() {
        try {
            return _resource.contentLength();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get content length of resource: " + _resource, e);
        }
    }

    @Override
    public boolean isExists() {
        return _resource.exists();
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public InputStream read() throws ResourceException {
        try {
            return _resource.getInputStream();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to get input stream of resource: " + _resource, e);
        }
    }

    @Override
    public void read(Action<InputStream> action) throws ResourceException {
        final InputStream in = read();
        try {
            action.run(in);
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in read callback", e);
        } finally {
            FileHelper.safeClose(in);
        }
    }

    @Override
    public <E> E read(Func<InputStream, E> func) throws ResourceException {
        final InputStream in = read();
        try {
            return func.eval(in);
        } catch (Exception e) {
            throw new ResourceException(this, "Error occurred in read callback", e);
        } finally {
            FileHelper.safeClose(in);
        }
    }

    @Override
    public void write(Action<OutputStream> arg0) throws ResourceException {
        throw new UnsupportedOperationException();
    }

}
