import React from 'react';

class MNISTDigitRecognition extends React.Component {
    handleFileUpload = (event) => {
        const file = event.target.files[0];
        // Handle file upload logic here
        // You can use fetch or any other method to send the file data to your server
        // For simplicity, I'll just log the file object
        console.log(file);
    }

    render() {
        return (
            <div>
                <h2>MNIST Digit Recognition</h2>
                <form action="http://127.0.0.1:7000/receive_input" method="post" encType="multipart/form-data">
                    <input type="file" name="image" onChange={this.handleFileUpload} />
                    <input type="submit" value="Upload" />
                </form>
            </div>
        );
    }
}

export default MNISTDigitRecognition;
