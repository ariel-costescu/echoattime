function ConsoleMessageProcessor() {
    return {
        process: (time, msg) => {
            console.log(`${time}: '${msg}'`);
        }
    }
}

export { ConsoleMessageProcessor }