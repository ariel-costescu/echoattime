function getPostBodyAsJson(req, callback) {
    let body = '';
    
    req.on('data', (chunk) => {
        body += chunk.toString();
    });

    req.on('end', () => {
        callback(JSON.parse(body));
    });    
}

export {getPostBodyAsJson};