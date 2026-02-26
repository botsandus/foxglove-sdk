"""Launch foxglove_mcap_player with configurable parameters."""

from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    return LaunchDescription([
        DeclareLaunchArgument('file', description='Path to the MCAP file'),
        DeclareLaunchArgument('port', default_value='8765',
                              description='Foxglove WebSocket server port'),
        DeclareLaunchArgument('host', default_value='127.0.0.1',
                              description='Foxglove WebSocket server host'),
        Node(
            package='foxglove_mcap_player',
            executable='foxglove_mcap_player',
            name='foxglove_mcap_player',
            output='screen',
            parameters=[{
                'file': LaunchConfiguration('file'),
                'port': LaunchConfiguration('port'),
                'host': LaunchConfiguration('host'),
            }],
        ),
    ])
