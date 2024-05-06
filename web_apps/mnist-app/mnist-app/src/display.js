import React from "react";

// display the digit returned by the server
class Display extends React.Component {
    render() {
        return (
            <div>
                <h2>Digit Recognition Result</h2>
                <p>Digit: {this.props.digit}</p>
            </div>
        );
    }
}

export default Display;